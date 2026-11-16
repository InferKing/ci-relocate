from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class GitLabModel(BaseModel):
    model_config = ConfigDict(extra="allow", strict=True)


class User(GitLabModel):
    name: str | None = None
    username: str | None = None
    avatar_url: str | None = None


class Project(GitLabModel):
    id: int | None = Field(default=None, gt=0)
    name: str | None = None
    path_with_namespace: str | None = None
    web_url: str | None = None

    @model_validator(mode="after")
    def identifiable(self):
        if not (self.id or self.web_url or self.path_with_namespace):
            raise ValueError("project must have id, web_url or path_with_namespace")
        return self


class BaseEvent(GitLabModel):
    project: Project
    user: User | None = None
    user_name: str | None = None
    user_avatar: str | None = None


class Commit(GitLabModel):
    id: str | None = None
    message: str | None = None
    title: str | None = None
    url: str | None = None


class PushEvent(BaseEvent):
    ref: str = Field(min_length=1)
    before: str | None = None
    after: str | None = None
    compare: str | None = None
    commits: list[Commit] = Field(default_factory=list)
    total_commits_count: int | None = Field(default=None, ge=0)


class MergeRequestAttributes(GitLabModel):
    iid: int = Field(gt=0)
    action: str = Field(min_length=1)
    title: str | None = None
    url: str | None = None
    source_branch: str | None = None
    target_branch: str | None = None
    state: str | None = None
    draft: bool | None = None
    work_in_progress: bool | None = None
    updated_at: str | None = None
    actioned_at: str | None = None
    oldrev: str | None = None
    last_commit: Commit | None = None


class Change(GitLabModel):
    previous: Any = None
    current: Any = None


class MergeRequestEvent(BaseEvent):
    object_attributes: MergeRequestAttributes
    changes: dict[str, Change] = Field(default_factory=dict)
    reviewers: list[User] | None = None
    assignees: list[User] | None = None


class NoteAttributes(GitLabModel):
    noteable_type: str = Field(min_length=1)
    note: str = ""
    url: str | None = None
    system: bool = False


class NoteTarget(GitLabModel):
    title: str | None = None
    message: str | None = None
    url: str | None = None
    source_branch: str | None = None
    target_branch: str | None = None


class NoteEvent(BaseEvent):
    object_attributes: NoteAttributes
    merge_request: NoteTarget | None = None
    issue: NoteTarget | None = None
    commit: Commit | None = None


class WikiAttributes(GitLabModel):
    action: str | None = None
    title: str | None = None
    slug: str | None = None
    url: str | None = None
    message: str | None = None


class WikiEvent(BaseEvent):
    object_attributes: WikiAttributes


EVENT_MODELS = {
    "Push Hook": PushEvent,
    "Merge Request Hook": MergeRequestEvent,
    "Note Hook": NoteEvent,
    "Wiki Page Hook": WikiEvent,
}


def validate_payload(event: str, payload: Any) -> dict[str, Any]:
    model = EVENT_MODELS.get(event, BaseEvent)
    return model.model_validate(payload).model_dump(exclude_unset=True)
