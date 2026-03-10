"""Adapter interfaces."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AdapterCapability:
    target: str
    runtime_style: str
    supported_execution_types: tuple[str, ...]


class BaseAdapter:
    target: str
    runtime_style: str

    def map_operator(self, execution_type: str, profile: dict) -> str:
        mapping = profile.get("operator_mapping", {})
        if execution_type not in mapping:
            raise ValueError(f"unsupported execution_type for {self.target}: {execution_type}")
        return str(mapping[execution_type])

    def capability(self, profile: dict) -> AdapterCapability:
        return AdapterCapability(
            target=self.target,
            runtime_style=self.runtime_style,
            supported_execution_types=tuple(profile.get("supported_execution_types", [])),
        )
