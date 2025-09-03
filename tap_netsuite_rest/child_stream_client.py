from tap_netsuite_rest.client import NetSuiteStream
from typing import Optional, Iterable

class DynamicChildStreamClient(NetSuiteStream):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ids = set()
        self.fetch_from_parent_stream = False
    
    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        # 1. fetch all records using rep key
        if not self.fetch_from_parent_stream:
            yield from super().request_records(context)
            self.fetch_from_parent_stream = True
        # 2. fetch invoices from parent stream
        if self.fetch_from_parent_stream and not self.ignore_parent_stream:
            # get invoiceitem ids
            yield from super().request_records(context)
    
    def prepare_request_payload(self, context, next_page_token):
        if self.fetch_from_parent_stream and not self.ignore_parent_stream:
            # fetch reference data records when acting as a child stream
            ids = ', '.join(f"'{id}'" for id in context[self.child_context_key]) or "NULL"
            self.custom_filter = f"{self.table}.{self.primary_keys[0]} IN ({ids})"
            
            # Temporarily disable date filtering by clearing replication_key
            original_replication_key = self.replication_key
            self.replication_key = None
            
            try:
                return super().prepare_request_payload(context, next_page_token)
            finally:
                # Restore the original replication_key
                self.replication_key = original_replication_key
        
        return super().prepare_request_payload(context, next_page_token)

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        row_id = row[self.primary_keys[0]]
        if row_id not in self.ids:
            row = super().post_process(row, context)
            self.ids.add(row_id)
            return row