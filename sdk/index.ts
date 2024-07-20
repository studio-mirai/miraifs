import { Transaction } from "@mysten/sui/transactions";
import { Ed25519Keypair } from "@mysten/sui/keypairs/ed25519";
import { fromHEX } from "@mysten/sui/utils";
import { getFullnodeUrl, SuiClient } from "@mysten/sui/client";
import { getFaucetHost, requestSuiFromFaucetV0 } from "@mysten/sui/faucet";

const secret = "0x...";
const keypair = Ed25519Keypair.fromSecretKey(fromHEX(secret));

const rpcUrl = getFullnodeUrl("devnet");
const client = new SuiClient({ url: rpcUrl });
