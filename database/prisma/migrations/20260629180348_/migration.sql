-- CreateTable
CREATE TABLE "flipcash_currency_push_states" (
    "mint" TEXT NOT NULL,
    "allTimeHighSupply" BIGINT NOT NULL DEFAULT 0,
    "allTimeHighSlot" BIGINT NOT NULL DEFAULT 0,
    "lastGainPushAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "flipcash_currency_push_states_pkey" PRIMARY KEY ("mint")
);
