"""Target adapters and registry."""

from __future__ import annotations

from pipeline_codegen.adapters.base import BaseAdapter
from pipeline_codegen.errors import MappingError


class AirflowAdapter(BaseAdapter):
    target = "airflow"
    runtime_style = "imperative"


class PrefectAdapter(BaseAdapter):
    target = "prefect"
    runtime_style = "imperative"


class DagsterAdapter(BaseAdapter):
    target = "dagster"
    runtime_style = "imperative"


class KestraAdapter(BaseAdapter):
    target = "kestra"
    runtime_style = "declarative"


class KubeflowStubAdapter(BaseAdapter):
    target = "kubeflow"
    runtime_style = "declarative"


class KubernetesStubAdapter(BaseAdapter):
    target = "kubernetes"
    runtime_style = "declarative"


def get_adapter(target: str) -> BaseAdapter:
    adapters = {
        "airflow": AirflowAdapter(),
        "prefect": PrefectAdapter(),
        "dagster": DagsterAdapter(),
        "kestra": KestraAdapter(),
        "kubeflow": KubeflowStubAdapter(),
        "kubernetes": KubernetesStubAdapter(),
    }
    if target not in adapters:
        raise MappingError("MAP002", f"unsupported target: {target}")
    return adapters[target]
