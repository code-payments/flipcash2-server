-- CreateTable
CREATE TABLE "flipcash_iap" (
    "receiptId" TEXT NOT NULL,
    "platform" SMALLINT NOT NULL DEFAULT 0,
    "userId" TEXT NOT NULL,
    "product" SMALLINT NOT NULL DEFAULT 0,
    "paymentAmount" DOUBLE PRECISION NOT NULL,
    "paymentCurrency" TEXT NOT NULL,
    "state" SMALLINT NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "flipcash_iap_pkey" PRIMARY KEY ("receiptId")
);

-- CreateTable
CREATE TABLE "flipcash_publickeys" (
    "key" TEXT NOT NULL,
    "userId" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "flipcash_publickeys_pkey" PRIMARY KEY ("key")
);

-- CreateTable
CREATE TABLE "flipcash_pushtokens" (
    "userId" TEXT NOT NULL,
    "appInstallId" TEXT NOT NULL,
    "token" TEXT NOT NULL,
    "type" INTEGER NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "flipcash_pushtokens_pkey" PRIMARY KEY ("userId","appInstallId")
);

-- CreateTable
CREATE TABLE "flipcash_rendezvous" (
    "key" TEXT NOT NULL,
    "address" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    "expiresAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "flipcash_rendezvous_pkey" PRIMARY KEY ("key")
);

-- CreateTable
CREATE TABLE "flipcash_users" (
    "id" TEXT NOT NULL,
    "displayName" TEXT,
    "phoneNumber" TEXT,
    "emailAddress" TEXT,
    "isStaff" BOOLEAN NOT NULL DEFAULT false,
    "isRegistered" BOOLEAN NOT NULL DEFAULT false,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "flipcash_users_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "flipcash_x_profiles" (
    "id" TEXT NOT NULL,
    "username" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "description" TEXT NOT NULL,
    "profilePicUrl" TEXT NOT NULL,
    "followerCount" INTEGER NOT NULL DEFAULT 0,
    "verifiedType" SMALLINT NOT NULL DEFAULT 0,
    "accessToken" TEXT NOT NULL,
    "userId" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "flipcash_x_profiles_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "flipcash_publickeys_userId_key" ON "flipcash_publickeys"("userId");

-- CreateIndex
CREATE UNIQUE INDEX "flipcash_x_profiles_username_key" ON "flipcash_x_profiles"("username");

-- CreateIndex
CREATE UNIQUE INDEX "flipcash_x_profiles_userId_key" ON "flipcash_x_profiles"("userId");

-- AddForeignKey
ALTER TABLE "flipcash_publickeys" ADD CONSTRAINT "flipcash_publickeys_userId_fkey" FOREIGN KEY ("userId") REFERENCES "flipcash_users"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "flipcash_x_profiles" ADD CONSTRAINT "flipcash_x_profiles_userId_fkey" FOREIGN KEY ("userId") REFERENCES "flipcash_users"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
