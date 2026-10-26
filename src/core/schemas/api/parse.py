from core.schemas.parse import ParseEnqueueInDto, ParseEnqueueOutDto


class ParseRunInDto(ParseEnqueueInDto):
    """Peer command; extra fields are forbidden by the command contract."""


class ParseRunOutDto(ParseEnqueueOutDto):
    """Public run projection without credentials or account identifiers."""
