# Future service API notes

The current project is still primarily a Python library, but the runtime model
already maps well to a future web service.

## Likely resources

- workflow descriptions
- runs
- task instances
- task logs
- cache entries
- backend job metadata

## Likely actions

- submit a run
- inspect run status
- inspect task status
- cancel a run
- retry failed tasks
- browse historical runs

## Why this stays separate for now

The current docs focus on the library itself. Once the web app exists, the HTTP
surface can get its own OpenAPI schema and ReDoc or Swagger view.
