> cd Docker

### Build and start MainModule
> docker compose -f docker-compose.base.yml up -d --build

### Stop MainModule container
> docker compose -f docker-compose.base.yml down

<!-- ### Build and start RecordLinkage and MainModule
> docker compose -f docker-compose.yml up -d --build

### Stop them
> docker compose -f docker-compose.yml down

### Build and start RecordLinkage
> docker compose -f docker-compose.rl.yml up -d --build -->