from __future__ import annotations

import json
import os
import shutil
import traceback
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable
from uuid import uuid4

from ..models import utc_now_iso
from ..migrations import MigrationContext, build_migration_plan, resolve_chain
from ..storage import MemoryStorageV3

if TYPE_CHECKING:
    from ..engine_runtime import EngineRuntime


def _clamp_score(value: float) -> float:
    return max(0.0, min(1.0, float(value)))

class MigrationService:
    def __init__(
        self,
        runtime: "EngineRuntime",
        *,
        code_schema_version: Callable[[], int],
        engine_version: Callable[[], str],
        topology: Callable[[], Any],
    ) -> None:
        self.runtime = runtime
        self._code_schema_version = code_schema_version
        self._engine_version = engine_version
        self._topology_provider = topology

    @property
    def topology(self) -> Any:
        return self._topology_provider()

    @property
    def code_schema_version(self) -> int:
        return int(self._code_schema_version())

    @property
    def engine_version(self) -> str:
        return str(self._engine_version())

    @staticmethod
    def _migration_default_schema_version() -> int:
        return 1

    def _safe_append_migration_event(self, *, event_type: str, payload: dict[str, Any]) -> None:
        try:
            bucket_id = self.topology.active_bucket_id()
            self.runtime.storage.append_event(event_type=event_type, bucket_id=bucket_id, payload=dict(payload))
        except Exception:
            pass

    def _acquire_migration_lock(self, *, run_id: str) -> int:
        lock_path = self.runtime.storage.migration_lock_file
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        try:
            fd = os.open(str(lock_path), flags, 420)
        except FileExistsError as exc:
            raise RuntimeError(f'schema migration lock exists: {lock_path}; manual confirmation required before removing lock') from exc
        payload = {'run_id': run_id, 'pid': os.getpid(), 'created_at': utc_now_iso(), 'engine_version': self.engine_version, 'code_schema_version': self.code_schema_version}
        os.write(fd, (json.dumps(payload, ensure_ascii=False, indent=2) + '\n').encode('utf-8'))
        os.fsync(fd)
        return fd

    def _release_migration_lock(self, fd: int | None) -> None:
        try:
            if fd is not None:
                os.close(fd)
        except Exception:
            pass
        try:
            self.runtime.storage.migration_lock_file.unlink(missing_ok=True)
        except Exception:
            pass

    def _migration_status_unlocked(self) -> dict[str, Any]:
        code_version = self.code_schema_version
        default_version = self._migration_default_schema_version()
        schema_exists = bool(self.runtime.storage.schema_version_file.exists())
        schema_info = self.runtime.storage.read_schema_version(default_schema_version=default_version)
        data_version = int(schema_info.get('schema_version', default_version))
        plan: list[dict[str, Any]] = []
        plan_error = ''
        try:
            plan = build_migration_plan(from_version=data_version, to_version=code_version)
        except Exception as exc:
            plan_error = str(exc)
        return {'success': True, 'code_schema_version': code_version, 'data_schema_version': data_version, 'schema_file_exists': schema_exists, 'schema_info': schema_info, 'needs_migration': data_version != code_version, 'downgrade_blocked': data_version > code_version, 'lock_exists': bool(self.runtime.storage.migration_lock_file.exists()), 'plan': plan, 'plan_error': plan_error, 'journal': self.runtime.storage.load_migration_journal(), 'paths': {'schema_version_file': str(self.runtime.storage.schema_version_file), 'migration_journal_file': str(self.runtime.storage.migration_journal_file), 'migration_lock_file': str(self.runtime.storage.migration_lock_file), 'migration_tmp_dir': str(self.runtime.storage.migration_tmp_dir), 'pre_upgrade_backup_dir': str(self.runtime.storage.pre_upgrade_backup_dir)}}

    def _migrate_if_needed(self, *, force: bool, dry_run: bool) -> dict[str, Any]:
        if not isinstance(self.runtime.storage, MemoryStorageV3):
            return {'success': False, 'message': 'storage not initialized'}
        code_version = self.code_schema_version
        default_version = self._migration_default_schema_version()
        schema_exists = bool(self.runtime.storage.schema_version_file.exists())
        schema_info = self.runtime.storage.read_schema_version(default_schema_version=default_version)
        data_version = int(schema_info.get('schema_version', default_version))
        plan = build_migration_plan(from_version=data_version, to_version=code_version)
        if dry_run:
            return {'success': True, 'dry_run': True, 'code_schema_version': code_version, 'data_schema_version': data_version, 'needs_migration': data_version != code_version, 'plan': plan}
        if data_version > code_version:
            raise RuntimeError(f'memory data schema is newer than code: data={data_version}, code={code_version}; please upgrade program code')
        needs_migration = data_version < code_version
        if not needs_migration:
            if not schema_exists or str(schema_info.get('engine_version', '')).strip() != self.engine_version:
                self.runtime.storage.write_schema_version(schema_version=code_version, engine_version=self.engine_version)
            return {'success': True, 'migrated': False, 'code_schema_version': code_version, 'data_schema_version': data_version, 'plan': [], 'forced': bool(force)}
        run_id = f"migration_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_{uuid4().hex[:8]}"
        run_root = self.runtime.storage.migration_tmp_dir / f'run_{run_id}'
        workspace_root = run_root / 'workspace'
        checkpoints_root = run_root / 'checkpoints'
        rollback_root = run_root / 'rollback_live'
        lock_fd: int | None = None
        step_results: list[dict[str, Any]] = []
        pre_backup_dir = self.runtime.storage.pre_upgrade_backup_dir
        run_root.mkdir(parents=True, exist_ok=True)
        checkpoints_root.mkdir(parents=True, exist_ok=True)
        try:
            lock_fd = self._acquire_migration_lock(run_id=run_id)
            print(f'[memory-migration] start run_id={run_id} data={data_version} code={code_version}')
            self.runtime.storage.save_migration_journal({'status': 'running', 'run_id': run_id, 'started_at': utc_now_iso(), 'from_version': data_version, 'to_version': code_version, 'plan': plan, 'completed_steps': []})
            self._safe_append_migration_event(event_type='MIGRATION_START', payload={'run_id': run_id, 'from_version': data_version, 'to_version': code_version, 'plan': plan})
            self.runtime.storage.clone_live_dataset(pre_backup_dir)
            self.runtime.storage.clone_live_dataset(workspace_root)
            workspace_storage = MemoryStorageV3(workspace_root, evidence_versions=self.runtime._evidence_versions)
            workspace_storage.write_schema_version(schema_version=data_version, engine_version=self.engine_version)
            chain = resolve_chain(from_version=data_version, to_version=code_version)
            for idx, step in enumerate(chain, start=1):
                print(f'[memory-migration] step {idx}/{len(chain)} {step.id} ({step.from_version}->{step.to_version})')
                context = MigrationContext(run_id=run_id, from_version=int(step.from_version), to_version=int(step.to_version), workspace_root=workspace_root)
                apply_out = step.apply(storage=workspace_storage, context=context) or {}
                validate_out = step.validate(storage=workspace_storage, context=context) or {}
                workspace_storage.write_schema_version(schema_version=int(step.to_version), engine_version=self.engine_version)
                if int(step.to_version) >= 4 and workspace_storage.sqlite_index_file.exists():
                    workspace_storage.activate_v4()
                step_info = {'id': str(step.id), 'from_version': int(step.from_version), 'to_version': int(step.to_version), 'apply': dict(apply_out) if isinstance(apply_out, dict) else {'result': apply_out}, 'validate': dict(validate_out) if isinstance(validate_out, dict) else {'result': validate_out}, 'completed_at': utc_now_iso()}
                step_results.append(step_info)
                checkpoint_dir = checkpoints_root / f"step_{idx:03d}_{str(step.id).replace('/', '_')}"
                workspace_storage.clone_live_dataset(checkpoint_dir)
                workspace_storage.append_event(event_type='MIGRATION_STEP_DONE', bucket_id=workspace_storage.get_active_bucket_id(), payload={'run_id': run_id, 'step': step_info})
                self.runtime.storage.save_migration_journal({'status': 'running', 'run_id': run_id, 'started_at': utc_now_iso(), 'from_version': data_version, 'to_version': code_version, 'plan': plan, 'completed_steps': step_results, 'current_step': str(step.id)})
            workspace_storage.clear_query_cache()
            workspace_storage.write_schema_version(schema_version=code_version, engine_version=self.engine_version)
            check = workspace_storage.validate_dataset_layout(root_dir=workspace_root)
            if not bool(check.get('success', False)):
                raise RuntimeError(f'migration validation failed: {check}')
            workspace_storage.append_event(event_type='MIGRATION_SUCCESS', bucket_id=workspace_storage.get_active_bucket_id(), payload={'run_id': run_id, 'from_version': data_version, 'to_version': code_version, 'steps': step_results})
            workspace_storage.close()
            switch = self.runtime.storage.replace_live_dataset_from_workspace(workspace_root=workspace_root, rollback_root=rollback_root)
            if not bool(switch.get('success', False)):
                raise RuntimeError(f'dataset switch failed: {switch}')
            if self.runtime.storage.sqlite_index_file.exists():
                self.runtime.storage.activate_v4()
            self.runtime.storage.write_schema_version(schema_version=code_version, engine_version=self.engine_version)
            self.runtime.storage.save_migration_journal({'status': 'success', 'run_id': run_id, 'finished_at': utc_now_iso(), 'from_version': data_version, 'to_version': code_version, 'plan': plan, 'completed_steps': step_results, 'switch': switch})
            print(f'[memory-migration] success run_id={run_id}')
            return {'success': True, 'migrated': True, 'run_id': run_id, 'from_version': data_version, 'to_version': code_version, 'plan': plan, 'step_results': step_results}
        except Exception as exc:
            fail_payload = {'status': 'failed', 'run_id': run_id, 'failed_at': utc_now_iso(), 'from_version': data_version, 'to_version': code_version, 'plan': plan, 'completed_steps': step_results, 'error': str(exc), 'traceback': traceback.format_exc()}
            self.runtime.storage.save_migration_journal(fail_payload)
            self._safe_append_migration_event(event_type='MIGRATION_FAIL', payload={'run_id': run_id, 'from_version': data_version, 'to_version': code_version, 'error': str(exc)})
            print(f'[memory-migration] failed run_id={run_id}: {exc}')
            raise RuntimeError(f'schema migration failed: {exc}') from exc
        finally:
            self._release_migration_lock(lock_fd)
            if run_root.exists():
                shutil.rmtree(run_root, ignore_errors=True)

    async def migration_status(self) -> dict[str, Any]:
        async with self.runtime._global_meta_lock:
            return self._migration_status_unlocked()

    async def migrate_schema(self, *, dry_run: bool=False) -> dict[str, Any]:
        async with self.runtime._global_meta_lock:
            return self._migrate_if_needed(force=True, dry_run=bool(dry_run))

    async def migrate_storage_paths_to_relative(self) -> dict[str, int]:
        async with self.runtime._global_meta_lock:
            return await self.runtime.run_storage_task(self.runtime.storage.migrate_paths_to_relative)
