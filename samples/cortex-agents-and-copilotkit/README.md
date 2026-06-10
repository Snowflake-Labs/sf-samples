This is a [Snowflake](https://snowflake.com), [Next.js](https://nextjs.org), & [Copilotkit](https://www.copilotkit.ai/) project bootstrapped with [`create-next-app`](https://nextjs.org/docs/app/api-reference/cli/create-next-app).

## Getting Started

First, create a .env file with the following:
```txt
SNOWFLAKE_ACCOUNT=[YOUR SNOWFLAKE ACCOUNT]
SNOWFLAKE_USER=[YOUR SNOWFLAKE USER]
SNOWFLAKE_PASSWORD=[YOUR SNOWFLAKE PASSWORD]
SNOWFLAKE_WAREHOUSE=SUMMIT_GEAR_WH
SNOWFLAKE_DATABASE=SUMMIT_GEAR_CO
SNOWFLAKE_SCHEMA=SALES
SNOWFLAKE_AGENT_NAME=SUMMIT_GEAR_AGENT

# Generate this inside Snowsight under your User Profile -> Settings -> Authentication -> PAT
SNOWFLAKE_TOKEN=[YOUR SNOWFLAKE PAT TOKEN]
```

Next, run the development server:

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
# or
bun dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.
