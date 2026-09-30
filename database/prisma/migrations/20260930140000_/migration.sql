-- AlterTable
ALTER TABLE "flipcash_users" ADD COLUMN     "flipcardColor" TEXT;

-- Backfill: carry every colour already picked over from the deprecated column.
UPDATE "flipcash_users" SET "flipcardColor" = "tipCardColor" WHERE "tipCardColor" IS NOT NULL;
