from typing import Dict, Any, List

class Transform:
    def __init__(self):
        
        self.allowed_columns = {
            "deals": ["id", "name", "status", "total_price", "recurrence_price", "one_time_price", "expected_close_date", "closed_at", "created_at", "updated_at", "proposta_entregue", "organization_id", "owner_id", "pipeline_id", "stage_id", "source_id"],
            "contacts": ["id", "name", "job_title", "organization_id"],
            "organizations": ["id", "name", "cnpj", "razao_social", "cidade", "estado"],
            "users": ["id", "name"],
            "tasks": ["id", "name", "deal_id", "owner_id", "due_date", "completed_at", "updated_at"],
            "pipelines": ["id", "name"]
        }

    def _safe_get(self, obj: Any, key: str) -> Any:
        if isinstance(obj, dict):
            return obj.get(key)
        return None

    def _filter_columns(self, entity_name: str, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        allowed = self.allowed_columns.get(entity_name, [])
        filtered_records = []

        for r in records:
            filtered_row = {k: v for k, v in r.items() if k in allowed}
            filtered_records.append(filtered_row)

        return filtered_records

    def transform_generic(self, entity_name: str, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not isinstance(data, list):
            return []

        cleaned = []

        for item in data:
            if not isinstance(item, dict):
                continue
            cleaned.append(item)


        return self._filter_columns(entity_name, cleaned)

    def process(self, raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        return {
            "deals": self.transform_generic("deals", raw_data.get("deals")),
            "contacts": self.transform_generic("contacts", raw_data.get("contacts")),
            "organizations": self.transform_generic("organizations", raw_data.get("organizations")),
            "users": self.transform_generic("users", raw_data.get("users")),
            "tasks": self.transform_generic("tasks", raw_data.get("tasks")),
            "pipelines": self.transform_generic("pipelines", raw_data.get("pipelines")),
        }