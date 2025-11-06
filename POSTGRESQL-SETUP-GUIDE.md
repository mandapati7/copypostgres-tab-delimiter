# PostgreSQL Setup Guide (Without Docker)

This guide will help you set up PostgreSQL locally on your Windows machine for the PostgreSQL CSV Loader.

## 🚀 PostgreSQL Installation

### Step 1: Download PostgreSQL

1. Visit [PostgreSQL Downloads](https://www.postgresql.org/download/windows/)
2. Download the PostgreSQL installer for Windows
3. Choose version 12+ (recommended: PostgreSQL 15 or 16)

### Step 2: Install PostgreSQL

1. Run the installer as Administrator
2. Follow the installation wizard:
   - **Installation Directory**: Keep default (`C:\Program Files\PostgreSQL\15`)
   - **Data Directory**: Keep default
   - **Password**: Set a strong password for the `postgres` user (remember this!)
   - **Port**: Keep default (5432)
   - **Locale**: Keep default

### Step 3: Verify Installation

1. Open Command Prompt as Administrator
2. Navigate to PostgreSQL bin directory:
   ```cmd
   cd "C:\Program Files\PostgreSQL\15\bin"
   ```
3. Test connection:
   ```cmd
   psql.exe -h az1ompdbvlut202 -p 5432 -d omp04u -U gmanda01
   ```
4. Enter the password you set during installation
5. You should see the PostgreSQL prompt: `postgres=#`

## 🔧 Application Configuration

### Step 1: Update application.properties

Edit `src/main/resources/application.properties`:

```properties
# Database Configuration - Update these values
spring.datasource.url=jdbc:postgresql://localhost:5432/postgres
spring.datasource.username=postgres
spring.datasource.password=YOUR_PASSWORD_HERE
spring.datasource.driver-class-name=org.postgresql.Driver
```

### Step 2: Create Application Database (Optional)

If you want a dedicated database for the application:

1. Connect to PostgreSQL:

   ```cmd
   psql.exe -h az1ompdbvlut202 -p 5432 -d omp04u -U gmanda01
   ```

2. Create a new database:

   ```sql
   CREATE DATABASE staging
   ```

3. Update your `application.properties`:
   ```properties
   spring.datasource.url=jdbc:postgresql://localhost:5432/staging
   ```

## 🎯 Environment-Specific Setup

### Development Environment

Create `application-dev.properties`:

```properties
spring.datasource.url=jdbc:postgresql://localhost:5432/staging
spring.datasource.username=postgres
spring.datasource.password=your_dev_password
logging.level.com.gk.ingest=DEBUG
```

Run with development profile:

```bash
mvn spring-boot:run -Dspring.profiles.active=dev
```

### Production Environment

For production deployment:

1. Set environment variables:

   ```cmd
   set DATABASE_URL=jdbc:postgresql://your-server:5432/production_db
   set DATABASE_USERNAME=production_user
   set DATABASE_PASSWORD=secure_production_password
   ```

2. Run with production profile:
   ```bash
   java -jar target/postgres-csv-loader-0.0.1-SNAPSHOT.jar --spring.profiles.active=prod
   ```

## 🧪 Testing the Setup

### Step 1: Start the Application

```bash
cd CopyPostgres
mvn spring-boot:run
```

### Step 2: Verify Database Connection

1. Open browser: `http://localhost:8081`
2. Click on "Database Connection Info" link
3. You should see connection status as "HEALTHY"

### Step 3: Initialize Staging Environment

1. Go to "Interactive Testing" tab
2. Click "Initialize Staging" button
3. Verify staging schema creation

## 🔍 Troubleshooting

### Connection Issues

**Error**: `Connection refused`

- **Solution**: Ensure PostgreSQL service is running
- **Check**: Services → PostgreSQL service should be "Running"

**Error**: `Authentication failed`

- **Solution**: Verify username/password in `application.properties`
- **Check**: Try connecting with `psql` first

**Error**: `Database does not exist`

- **Solution**: Create the database or use existing `postgres` database
- **Check**: List databases with `\l` in psql

### Port Issues

**Error**: `Port 8081 already in use`

- **Solution**: Change port in `application.properties`:
  ```properties
  server.port=8082
  ```

### Performance Issues

**Slow startup**: Increase JVM memory:

```bash
set JAVA_OPTS=-Xmx2g -Xms1g
mvn spring-boot:run
```

## 📋 Quick Reference

### Useful PostgreSQL Commands

```sql
-- List all databases
-- Shortcut:
\l
-- SQL:
SELECT datname FROM pg_database WHERE datistemplate = false;

-- Connect to database
-- Shortcut:
\c database_name

-- List all tables in current schema
-- Shortcut:
\dt
-- SQL:
SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';

-- List all schemas
-- Shortcut:
\dn
-- SQL:
SELECT schema_name FROM information_schema.schemata;

-- Show current user
SELECT current_user;

-- Show permissions for current user
SELECT grantee, table_catalog, table_schema, table_name, privilege_type
FROM information_schema.table_privileges
WHERE grantee = current_user;

-- Exit psql
\q
```

### Application URLs

- **Main Interface**: http://localhost:8081
- **Health Check**: http://localhost:8081/api/v1/ingest/health
- **Connection Info**: http://localhost:8081/api/v1/ingest/connection/info

---

**🎉 Your PostgreSQL CSV Loader is now ready to use without Docker!**
