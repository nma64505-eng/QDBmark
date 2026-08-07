# Security Policy

## Supported Versions

Security fixes are handled on the latest public release line.

## Reporting a Vulnerability

Please report vulnerabilities through GitHub issues if the report does not include secrets or exploit details. For sensitive reports, contact the repository owner privately first.

Do not include database passwords, SSH private keys, Prometheus credentials, customer reports, or production endpoints in public issues.

## Data Handling

QDBmark stores runtime data under `data/`, including reports, uploaded files, task artifacts, and settings. This directory is ignored by Git and should not be committed.

Release packages include a Docker image intended for direct deployment. Anyone with access to an image can inspect its filesystem, so do not store secrets in the image.
