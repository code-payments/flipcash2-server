-- AlterTable
ALTER TABLE "flipcash_users" ADD COLUMN     "username" TEXT;

-- CreateIndex
CREATE UNIQUE INDEX "flipcash_users_username_key" ON "flipcash_users"("username");
