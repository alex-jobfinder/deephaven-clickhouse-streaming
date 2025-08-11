curl -X POST https: //api.hyperliquid.xyz/info \
  -H "Content-Type: application/json" \
  -d '{
    "type": "l2Book",
    "coin": "BTC"
}'

curl -X POST https: //api.hyperliquid.xyz/info \
  -H "Content-Type: application/json" \
  -d '{
    "type": "l2Book",
    "coin": "BTC",
    "nSigFigs": 3
}'


curl -X POST https: //api.hyperliquid.xyz/info \
  -H "Content-Type: application/json" \
  -d '{
    "type": "spotMeta"
}'

curl -X POST https://api.hyperliquid.xyz/info \
  -H "Content-Type: application/json" \
  -d '{
    "type": "spotMetaAndAssetCtxs"
}'

Retrieve spot asset contexts

<!-- Retrieve perpetuals metadata (universe and margin tables) -->
curl -X POST https://api.hyperliquid.xyz/info \
  -H "Content-Type: application/json" \
  -d '{
    "type": "meta"
}'


metaAndAssetCtxs


POST https://api.hyperliquid.xyz/info
Headers
Name
Type
Description

Content-Type*

String

"application/json"
Request Body
Name
Type
Description

type*

String

"spotMetaAndAssetCtxs"

[
{
    "tokens": [
        {
            "name": "USDC",
            "szDecimals": 8,
            "weiDecimals" 8,
            "index": 0,
            "tokenId": "0x6d1e7cde53ba9467b783cb7c530ce054",
            "isCanonical": true,
            "evmContract":null,
            "fullName":null
        },
        {
            "name": "PURR",
            "szDecimals": 0,
            "weiDecimals": 5,
            "index": 1,
            "tokenId": "0xc1fb593aeffbeb02f85e0308e9956a90",
            "isCanonical": true,
            "evmContract":null,
            "fullName":null
        }
    ],
    "universe": [
        {
            "name": "PURR/USDC",
            "tokens": [1, 0],
            "index": 0,
            "isCanonical": true
        }
    ]
},
[
    {
        "dayNtlVlm":"8906.0",
        "markPx":"0.14",
        "midPx":"0.209265",
        "prevDayPx":"0.20432"
    }
]
]