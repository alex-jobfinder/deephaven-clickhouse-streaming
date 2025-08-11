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
