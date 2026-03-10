"""Structured error taxonomy."""

from __future__ import annotations


class CodegenError(ValueError):
    def __init__(self, code: str, message: str, path: str = "$") -> None:
        self.code = code
        self.message = message
        self.path = path
        super().__init__(f"{code} {path}: {message}")


class MappingError(CodegenError):
    pass


class TargetIRError(CodegenError):
    pass


class GenerationError(CodegenError):
    pass


class VerificationError(CodegenError):
    pass
