# Groups

Each subdirectory here represents a chat group/channel with isolated context:

- `main/` - Your private admin channel (full project access)
- `global/` - Shared read-only context for all groups
- `{group-name}/` - Other registered groups (isolated filesystem)

Each group folder contains:
- `CLAUDE.md` - Memory and instructions for that group
- `logs/` - Container execution logs
- `conversations/` - Archived conversation transcripts

These directories are gitignored since they contain personal data.
