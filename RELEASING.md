# Releasing

Releases ship three artifacts off one git tag:

| Artifact | Registry |
|---|---|
| `murmur-runtime` | [PyPI](https://pypi.org/project/murmur-runtime/) |
| `murmur-client` | [PyPI](https://pypi.org/project/murmur-client/) |
| `@murmur/client` | [npm](https://www.npmjs.com/package/@murmur/client) |

The whole pipeline runs from `.github/workflows/release.yml` on a `v*` tag push.

## One-time setup

These need to land before the **first** release. Subsequent releases
need none of this.

### 1. PyPI Trusted Publishers (no API tokens)

Add a Trusted Publisher entry on PyPI for each project. PyPI
verifies the GitHub OIDC token at publish time — no long-lived
credentials live in the repo.

For both `murmur-runtime` and `murmur-client`, register a publisher at:

- `https://pypi.org/manage/project/murmur-runtime/settings/publishing/`
- `https://pypi.org/manage/project/murmur-client/settings/publishing/`

Use these values:

| Field | Value |
|---|---|
| PyPI Project Name | `murmur-runtime` / `murmur-client` |
| Owner | `droidnoob` |
| Repository name | `murmur-runtime` |
| Workflow name | `release.yml` |
| Environment name | `pypi` |

If the project doesn't exist on PyPI yet, register it as a "pending"
trusted publisher first — PyPI claims the name on the first successful
publish.

### 2. GitHub Environment

Create a `pypi` environment on the repository
(`Settings → Environments → New environment`). The release workflow
references it explicitly so PyPI's Trusted Publisher binding has a
concrete environment to match against. No protection rules are
required, but adding "Required reviewers" gates the first publish on a
manual approval if you want a safety net.

### 3. npm token

The npm side uses a classic automation token (npm doesn't yet have
trusted publishing).

1. Create a token: <https://www.npmjs.com/settings/droidnoob/tokens>
   → "Generate New Token" → **Automation** type.
2. Add it to GitHub Secrets at
   `Settings → Secrets and variables → Actions → New repository secret`:
   - Name: `NPM_TOKEN`
   - Value: the token

The first publish needs `--access public` (the workflow passes it
automatically; required because `@murmur/...` is a scoped package
and scoped packages default to private).

## Cutting a release

Once setup is complete, releasing is one tag push.

```bash
# 1. Bump versions in lockstep.
#    Each release must touch ALL of these:
#      pyproject.toml                                  → version = "X.Y.Z"
#      src/murmur/__init__.py                          → __version__ = "X.Y.Z"
#      packages/murmur-client/pyproject.toml           → version = "X.Y.Z"
#      packages/js-client/package.json                 → "version": "X.Y.Z"

# 2. Commit the bump.
git add -A
git commit -m "release: 0.2.0"

# 3. Tag and push.
git tag v0.2.0
git push origin main --tags
```

The `verify` job in the workflow fails fast if the tag and any of the
four version declarations disagree, so a typo in one place won't ship a
mismatched release.

After the tag push, the workflow:

1. **`verify`** — confirms tag and in-tree versions agree.
2. **`pypi`** — builds + publishes `murmur-runtime` and `murmur-client`
   (Trusted Publishing OIDC).
3. **`npm`** — typechecks, tests, builds, publishes
   `@murmur/client` with provenance.
4. **`github-release`** — creates a GitHub Release with auto-generated
   notes pulled from commit subjects since the previous tag.

Failures are non-destructive — the tag stays, you fix the underlying
issue and either bump the patch (`v0.2.1`) or, if nothing has shipped
yet, delete the tag (`git push --delete origin v0.2.0` and locally) and
retry.

## Skipping a publisher

To ship to only one registry — say, a Python-only release — comment out
the `npm` job's `publish` step or use a `workflow_dispatch` variant.
The current workflow always publishes all three; a finer-grained
"matrix of which-registry-to-hit" mechanism is not built.

## Manual emergency publish

If GitHub Actions is down, the same commands run locally:

```bash
# PyPI
uv build && uv publish --token "$PYPI_TOKEN"
uv build packages/murmur-client && uv publish --token "$PYPI_TOKEN" packages/murmur-client/dist/*

# npm
cd packages/js-client && npm run build && npm publish --access public
```

Trusted Publishing won't work locally (no GitHub OIDC token); fall back
to a classic API token from <https://pypi.org/manage/account/token/>.
