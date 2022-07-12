package main

import (
	"fmt"
	gsrpc "github.com/centrifuge/go-substrate-rpc-client/v4"
	"github.com/centrifuge/go-substrate-rpc-client/v4/signature"
	"github.com/centrifuge/go-substrate-rpc-client/v4/types"
	"math/big"
)

//
//func main() {
//	sender, err := signature.KeyringPairFromSecret("ethics furnace bird nerve mean call title blossom ill verify grass proud", 42) // test4
//	if err != nil {
//		panic(err)
//	}
//	api, err := gsrpc.NewSubstrateAPI("ws://127.0.0.1:8546")
//	if err != nil {
//		fmt.Println(err)
//	}
//	hash, err := api.RPC.Chain.GetBlockHashLatest()
//	if err != nil {
//		fmt.Println(err)
//	}
//	fmt.Println("Hash: ", hash)
//
//	meta, err := api.RPC.State.GetMetadataLatest()
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println("Got meta")
//
//	test2Account, err := types.NewAddressFromHexAccountID("4617579c92b915a76ade267fb42ba5a28f10b51686f84aa968b0270ea2fc7243") // TEST2 account
//	if err != nil {
//		panic(err)
//	}
//
//	c, err := types.NewCall(meta, "Balances.transfer", test2Account, types.NewUCompactFromUInt(1000000000000))
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println("call works")
//
//	ext := types.NewExtrinsic(c)
//
//	genesisHash, err := api.RPC.Chain.GetBlockHash(0)
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println("got block hash")
//	rv, err := api.RPC.State.GetRuntimeVersionLatest()
//	if err != nil {
//		panic(err)
//	}
//
//	key, err := types.CreateStorageKey(meta, "System", "Account", sender.PublicKey, nil)
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println("created storage key")
//	var accountInfo types.AccountInfo
//	ok, err := api.RPC.State.GetStorageLatest(key, &accountInfo)
//	if err != nil || !ok {
//		fmt.Println(err)
//		panic(err)
//	}
//	fmt.Println("got storage latest")
//
//	options := types.SignatureOptions{
//		BlockHash:          genesisHash,
//		Era:                types.ExtrinsicEra{IsMortalEra: false},
//		GenesisHash:        genesisHash,
//		Nonce:              types.NewUCompactFromUInt(uint64(accountInfo.Nonce)),
//		SpecVersion:        rv.SpecVersion,
//		Tip:                types.NewUCompactFromUInt(0),
//		TransactionVersion: rv.TransactionVersion,
//	}
//	fmt.Println("options")
//	err = ext.Sign(sender, options)
//	if err != nil {
//		fmt.Println(err)
//		panic(err)
//	}
//
//	fmt.Println("signed")
//
//	hash, err = api.RPC.Author.SubmitExtrinsic(ext)
//	if err != nil {
//		panic(err)
//	}
//
//	fmt.Printf("Transfer sent with hash %#x\n", hash)
//}

func main() {
	// This sample shows how to create a transaction to make a transfer from one an account to another.

	// Instantiate the API
	api, err := gsrpc.NewSubstrateAPI("ws://127.0.0.1:8546")
	if err != nil {
		panic(err)
	}

	meta, err := api.RPC.State.GetMetadataLatest()
	if err != nil {
		panic(err)
	}

	// Create a call, transferring 12345 units to Bob
	bob, err := types.NewMultiAddressFromHexAccountID("0x8eaf04151687736326c9fea17e25fc5287613693c912909cb226aa4794f26a48")
	if err != nil {
		panic(err)
	}

	// 1 unit of transfer
	bal, ok := new(big.Int).SetString("100000000000000", 10)
	if !ok {
		panic(fmt.Errorf("failed to convert balance"))
	}

	c, err := types.NewCall(meta, "Balances.transfer", bob, types.NewUCompact(bal))
	if err != nil {
		panic(err)
	}

	// Create the extrinsic
	ext := types.NewExtrinsic(c)

	genesisHash, err := api.RPC.Chain.GetBlockHash(0)
	if err != nil {
		panic(err)
	}

	rv, err := api.RPC.State.GetRuntimeVersionLatest()
	if err != nil {
		panic(err)
	}

	key, err := types.CreateStorageKey(meta, "System", "Account", signature.TestKeyringPairAlice.PublicKey)
	if err != nil {
		panic(err)
	}

	var accountInfo types.AccountInfo
	ok, err = api.RPC.State.GetStorageLatest(key, &accountInfo)
	if err != nil || !ok {
		panic(err)
	}

	nonce := uint32(accountInfo.Nonce)
	o := types.SignatureOptions{
		BlockHash:          genesisHash,
		Era:                types.ExtrinsicEra{IsMortalEra: false},
		GenesisHash:        genesisHash,
		Nonce:              types.NewUCompactFromUInt(uint64(nonce)),
		SpecVersion:        rv.SpecVersion,
		Tip:                types.NewUCompactFromUInt(100),
		TransactionVersion: rv.TransactionVersion,
	}

	// Sign the transaction using Alice's default account
	err = ext.Sign(signature.TestKeyringPairAlice, o)
	if err != nil {
		panic(err)
	}

	// Send the extrinsic
	_, err = api.RPC.Author.SubmitExtrinsic(ext)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Balance transferred from Alice to Bob: %v\n", bal.String())
}
