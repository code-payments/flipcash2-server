/*
  Warnings:

  - A unique constraint covering the columns `[phoneNumber]` on the table `flipcash_users` will be added. If there are existing duplicate values, this will fail.
  - A unique constraint covering the columns `[phoneNumberHash]` on the table `flipcash_users` will be added. If there are existing duplicate values, this will fail.
  - A unique constraint covering the columns `[emailAddress]` on the table `flipcash_users` will be added. If there are existing duplicate values, this will fail.

*/
-- AlterTable
ALTER TABLE "flipcash_users" ADD COLUMN     "phoneNumberHash" TEXT;

-- CreateIndex
CREATE UNIQUE INDEX "flipcash_users_phoneNumber_key" ON "flipcash_users"("phoneNumber");

-- CreateIndex
CREATE UNIQUE INDEX "flipcash_users_phoneNumberHash_key" ON "flipcash_users"("phoneNumberHash");

-- CreateIndex
CREATE UNIQUE INDEX "flipcash_users_emailAddress_key" ON "flipcash_users"("emailAddress");
