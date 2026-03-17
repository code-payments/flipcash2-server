-- AlterTable
ALTER TABLE "flipcash_users" ADD COLUMN     "locale" TEXT NOT NULL DEFAULT 'en',
ADD COLUMN     "region" TEXT NOT NULL DEFAULT 'usd';
