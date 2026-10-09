import os
import time

import httpx
from northflank import ApiCallError, ApiClient


class _TransportFailure(RuntimeError):
    pass


class _SingleAttemptHttpClient(httpx.Client):
    def request(self, *args, **kwargs):
        try:
            return super().request(*args, **kwargs)
        except httpx.HTTPError:
            # SDK 2.0.0 retries every HTTPError, including uncertain POSTs.
            # Leave retry decisions to the adapter without patching SDK internals.
            raise _TransportFailure("Northflank transport failed") from None


class ApiError(RuntimeError):
    def __init__(self, operation, status=None):
        self.status = status
        self.retryable = status is None or status == 429 or status >= 500
        super().__init__(f"Northflank {operation} failed (HTTP {status or 'unavailable'})")


class NorthflankClient:
    def __init__(self, project_id, token=None, *, base_url="https://api.northflank.com"):
        self.token = token or os.environ.get("NF_API_TOKEN")
        if not self.token:
            raise ValueError("Set NF_API_TOKEN to a project-scoped Northflank API token")
        self.project_id = project_id
        self._http = _SingleAttemptHttpClient(timeout=30.0)
        self.sdk = ApiClient(
            api_token=self.token,
            base_url=base_url,
            user_agent="ray-northflank/0.1.0",
            throw_on_http_error=True,
            http_client=self._http,
        )

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._http.close()

    def _call(self, operation, endpoint, *, retry=True, **kwargs):
        attempts = 3 if retry else 1
        for attempt in range(attempts):
            try:
                return endpoint(project_id=self.project_id, **kwargs)
            except ApiCallError as error:
                failure = ApiError(operation, error.status)
            except _TransportFailure:
                failure = ApiError(operation)

            if not failure.retryable or attempt == attempts - 1:
                raise failure from None
            time.sleep(2 ** attempt)

    def list_services(self):
        services = []
        cursor = None
        seen = set()
        while True:
            response = self._call(
                "list services", self.sdk.list.services, per_page=100, cursor=cursor,
            )
            services.extend(response.data["services"])
            pagination = response.pagination
            if pagination is None:
                raise RuntimeError("Northflank omitted service pagination metadata")
            if not pagination.has_next_page:
                return services
            cursor = pagination.cursor
            if not cursor or cursor in seen:
                raise RuntimeError("Northflank returned an invalid pagination cursor")
            seen.add(cursor)

    def get_service(self, service_id):
        try:
            return self._call(
                "get service", self.sdk.get.service, service_id=service_id,
            ).data
        except ApiError as error:
            if error.status == 404:
                return None
            raise

    def create_service(self, body):
        created = self._call(
            "create service", self.sdk.create.service.deployment, retry=False, data=body,
        ).data
        service = self.get_service(body["name"])
        # Northflank can omit createdAt on CREATE; compare it when supplied.
        if not service or not service.get("uid"):
            raise RuntimeError(f"Could not confirm created service {body['name']}")
        identity_fields = ["id", "appId"]
        if created.get("createdAt"):
            identity_fields.append("createdAt")
        same_creation = all(created.get(key) and created[key] == service.get(key)
                            for key in identity_fields)
        if not same_creation or service["id"] != body["name"]:
            raise RuntimeError(f"Service identity changed during creation: {body['name']}")
        return service

    def get_runtime_environment(self, service_id):
        return self._call(
            "get service environment", self.sdk.get.service.runtime_environment,
            service_id=service_id, show="this",
        ).data["runtimeEnvironment"]

    def patch_service(self, service_id, body):
        self._call(
            "update service", self.sdk.patch.service.deployment,
            retry=False, service_id=service_id, data=body,
        )

    def set_description(self, service_id, description):
        self._call(
            "patch service", self.sdk.patch.service.deployment,
            retry=False, service_id=service_id, data={"description": description},
        )

    def delete_service(self, service_id):
        try:
            # The provider must read and verify the UID again before retrying a delete.
            self._call("delete service", self.sdk.delete.service, retry=False, service_id=service_id)
        except ApiError as error:
            if error.status != 404:
                raise
