> cd Docker

### Build and start dataset variation
> docker compose -f docker-compose.dv.yml up -d --build

### Stop dataset variation container
> docker compose -f docker-compose.dv.yml down

<!-- ### Build and start RecordLinkageInterface and dataset variation
> docker compose -f docker-compose.yml up -d --build

### Stop them
> docker compose -f docker-compose.yml down

### Build and start RecordLinkageInterface
> docker compose -f docker-compose.rl.yml up -d --build -->