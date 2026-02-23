kubectl exec debezium-cdc -n dca-rtm-staging -- pkill -f debezium_zerobus_bridge 2>&1; echo "old bridge killed"
kubectl cp /home/hongy/git/lakehouse/test01/env.sh debezium-cdc:/home/env.sh -n dca-rtm-staging && kubectl cp /home/hongy/git/lakehouse/test01/debezium/debezium_zerobus_bridge.py debezium-cdc:/home/debezium_zerobus_bridge.py -n dca-rtm-staging
## uncomment below to restart from begining
kubectl exec debezium-cdc -n dca-rtm-staging -- redis-cli XGROUP DESTROY tutorial.public.slurm_nodes zerobus-bridge 2>/dev/null; echo "reset Redis to restart"
kubectl exec -it debezium-cdc -n dca-rtm-staging -- bash -c "source /home/env.sh && python3 /home/debezium_zerobus_bridge.py run"

