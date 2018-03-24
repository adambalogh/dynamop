consul:
	bash -c "consul agent -dev -config-dir=/etc/consul.d"

server:
	bash -c "./build/install/dynamo/bin/worker-server $(port)"

client:
	bash -c "./build/install/dynamo/bin/client $(port)"
