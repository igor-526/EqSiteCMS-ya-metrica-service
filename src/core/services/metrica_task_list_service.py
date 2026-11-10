from core.protocols.repositories.metrica_task_list import MetricaTaskListRepositoryProtocol
from core.schemas.api.metrica_tasks import MetricaTaskListInDto, MetricaTaskListOutDto, MetricaTaskOutDto


class MetricaTaskListService:
    def __init__(self, repository: MetricaTaskListRepositoryProtocol) -> None:
        self._repository = repository

    async def list_tasks(self, *, data: MetricaTaskListInDto) -> MetricaTaskListOutDto:
        page = await self._repository.list_tasks(
            equestrian_id=data.equestrian_id, limit=data.limit, offset=data.offset
        )
        return MetricaTaskListOutDto(
            items=[MetricaTaskOutDto.model_validate(item) for item in page.items], total=page.total
        )
