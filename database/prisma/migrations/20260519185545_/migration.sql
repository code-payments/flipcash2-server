-- CreateTable
CREATE TABLE "flipcash_contact_lists" (
    "userId" TEXT NOT NULL,
    "checksum" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "flipcash_contact_lists_pkey" PRIMARY KEY ("userId")
);

-- CreateTable
CREATE TABLE "flipcash_contact_list_entries" (
    "userId" TEXT NOT NULL,
    "phoneNumberHash" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "flipcash_contact_list_entries_pkey" PRIMARY KEY ("userId","phoneNumberHash")
);

-- CreateIndex
CREATE INDEX "flipcash_contact_list_entries_phoneNumberHash_idx" ON "flipcash_contact_list_entries"("phoneNumberHash");

-- AddForeignKey
ALTER TABLE "flipcash_contact_lists" ADD CONSTRAINT "flipcash_contact_lists_userId_fkey" FOREIGN KEY ("userId") REFERENCES "flipcash_users"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "flipcash_contact_list_entries" ADD CONSTRAINT "flipcash_contact_list_entries_userId_fkey" FOREIGN KEY ("userId") REFERENCES "flipcash_users"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
