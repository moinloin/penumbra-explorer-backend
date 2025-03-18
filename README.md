<p align="center">
  <br />
  <span style="font-size: 32px; font-weight: bold;">Penumbra Explorer Backend</span>
</p>
<p align="center">
  A high-performance backend indexer for exploring the Penumbra blockchain built with Rust
</p>
<p align="center">
  <a href="https://github.com/tokio-rs/tokio">
    <img src="https://img.shields.io/badge/powered%20by-tokio-blue?style=flat&logo=rust" alt="Powered by Tokio" />
  </a>
  <a href="https://github.com/penumbra-zone/penumbra/tree/main/crates/bin/pindexer">
    <img src="https://img.shields.io/badge/built%20with-pindexer-blueviolet?style=flat" alt="Built with Pindexer" />
  </a>
  <a href="https://github.com/penumbra-zone/penumbra/tree/main/crates/util/cometindex">
    <img src="https://img.shields.io/badge/index%20by-cometindex-6A0DAD?style=flat" alt="Index by CometIndex" />
  </a>
  <a href="https://www.postgresql.org/">
    <img src="https://img.shields.io/badge/database-postgresql-blue?style=flat&logo=postgresql" alt="Database PostgreSQL" />
  </a>
  <a href="https://www.docker.com/">
    <img src="https://img.shields.io/badge/containerized-docker-2496ED?style=flat&logo=docker" alt="Containerized with Docker" />
  </a>
  <br />
  <br />
  <br />
</p>


## Features

- Indexes Penumbra blockchain data from a source database
- Stores processed data in a destination database
- Exposes a GraphQL API for querying blockchain data
- Supports block details, transactions, and search functionality


## Environment Setup

1. Copy the example environment file:
   ```sh
   cp .env.example .env
   ```

2. Update the `.env` file with your configuration:
    - Set `SOURCE_DB_URL` to your Penumbra node's database
    - Configure other settings as needed

3. Place your Genesis JSON file in the project root as `genesis.json`

## Local Development

### Running with Docker Compose

1. Start the application with databases:
   ```sh
   docker-compose up -d
   ```

2. Follow the logs:
   ```sh
   docker-compose logs -f app
   ```

3. Access the GraphQL API at http://localhost:3000/graphql

### Running without Docker

1. Set up a PostgreSQL database for the destination

2. Build and run the application:
   ```sh
   cargo build --release
   RUST_LOG=info ./target/release/penumbra-explorer \
       -s "postgresql://user:password@source-host:5432/source-db?sslmode=require" \
       -d "postgresql://user:password@dest-host:5432/dest-db" \
       --genesis-json "/path/to/genesis.json"
   ```

## API Endpoints

- **GraphQL API**: `/graphql`
- **GraphQL Playground**: `/graphql/playground`
- **Health Check**: `/health`

## Deployment

### Google Cloud Run Deployment

This project is configured for deployment to Google Cloud Run using GitHub Actions:

1. Add the following secrets to your GitHub repository:
    - `GCP_CREDENTIALS`: Google Cloud service account credentials JSON
    - `GCP_PROJECT_ID`: Your Google Cloud project ID
    - `GCP_REGION`: Deployment region (e.g., `europe-west6`)

2. Push to the `main` branch to trigger the deployment workflow

### Manual Deployment

1. Build the Docker image:
   ```sh
   docker build -t penumbra-explorer .
   ```

2. Deploy to your container platform of choice, providing the required environment variables

## Configuration

### Command Line Arguments

- `-s, --source-db-url`: Source database URL (required)
- `-d, --dest-db-url`: Destination database URL (required)
- `--genesis-json`: Path to Genesis JSON file (required)
- `--from-height`: Starting block height (optional)
- `--to-height`: Ending block height (optional)
- `--batch-size`: Batch size for processing blocks (default: 100)
- `--polling-interval-ms`: Polling interval in milliseconds (default: 1000)

## License

© 2024 PK Labs