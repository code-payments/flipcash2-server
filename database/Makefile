# Load environment variables from .env
ifneq (,$(wildcard ./.env))
    include .env
    export $(shell sed 's/=.*//' .env)
endif

.PHONY: format migrate deploy studio generate pull

# Format the Prisma schema file and report any errors
format:
	go run github.com/steebchen/prisma-client-go format

# Create a new migration file (without applying it)
migrate:
	go run github.com/steebchen/prisma-client-go migrate dev --create-only --skip-generate

# Apply any pending migrations to update the database schema in production/staging
deploy:
	go run github.com/steebchen/prisma-client-go migrate deploy

# Run Prisma Studio to view the database
studio:
#   Currently, the Prisma Studio is not working with the Go client. There is a
#   ticket open against Prisma to fix this issue but until that time, we're
#   going to use the JS client to run Prisma Studio.
#
#   See: https://github.com/prisma/studio/issues/1176
	@npm install
	npx prisma studio

#   If you want to use the Go client, you can run the following command:
#   go run github.com/steebchen/prisma-client-go studio

# Generate the Prisma Go client
 generate:
	go run github.com/steebchen/prisma-client-go generate

# Sync database with schema (development)
# push:
# 	go run github.com/steebchen/prisma-client-go db push

# Pull the database schema into Prisma schema
# pull:
# 	go run github.com/steebchen/prisma-client-go db pull

# Run a local test database using Docker
# (don't use this running golang tests, those spin up their own db)
db:
	docker run --name my-postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres