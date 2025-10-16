# Cortex Analyst Public Demo

## 🚀 How to Run the Project

### ✅ Prerequisites

* Ensure you have **key-pair authentication** set up for Snowflake.
  👉 Refer to the official guide:
  [🔐 Snowflake Key Pair Authentication Documentation](https://docs.snowflake.com/en/user-guide/key-pair-auth)

---

### ⚙️ Setup

1. **Create a temporary folder to store your private key:**

   ```bash
   mkdir tmp
   ```

2. **Place your private key file in the folder as:**

   ```
   tmp/rsa_key.p8
   ```

3. **Edit the configuration file:**

   Update `cfg.yaml` to point to your own **semantic model YAML file**.

4. **Create a Makefile:**

   Copy the structure of `Makefile.example` and modify it for your environment:

   * Set `SNOWFLAKE_PUBLIC_KEY_FP` to your **RSA public key fingerprint**.

   To retrieve the fingerprint, run the following in Snowsight:

   ```sql
   DESC USER example_user;

   SELECT SUBSTR(
     (SELECT "value"
      FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
      WHERE "property" = 'RSA_PUBLIC_KEY_FP'),
     LEN('SHA256:') + 1
   );
   ```

---

### 🖥️ Running the Backend

From the root of the project:

1. Initialize Go modules (if not already done):

   ```bash
   go mod init cortex-analyst-demo
   go mod tidy
   ```

2. Run the backend:

   ```bash
   make run
   ```

---

### 🌐 Running the Frontend

1. Navigate to the `cortexanalystapp/ui` directory:

   ```bash
   cd ui
   ```

2. Install dependencies:

   ```bash
   npm install
   ```

3. (Optional) Fix any audit issues:

   ```bash
   npm audit fix
   ```

4. Start the development server:

   ```bash
   npm run dev
   ```

If you make changes and want them to be reflected in the backend,
you need to run `npm run build` and check in the generated files.