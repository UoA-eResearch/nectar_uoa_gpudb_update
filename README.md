# nectar_uoa_gpudb_update

Collect data on UoA Nectar GPUs and update the local gpu database.

Run from crontab each hour on ntr-ops
```
5 * * * * /home/ntradm/bin/cron_update_ip2project
```

Configs currently in ~/etc
Binary currently in ~/bin

The gpu db receives gpu usage logs from each GPU instance, which is then used to generate GPU usage graphs in grafana. The data collected by this tool is needed to associate the usage logs, which is indexed by ip address, with specific GPUs on a hypervisor.

gpu db is also queried to generate the gpu web calendar.

This code is derived from Jason He's CLI gpu.py https://github.com/UoA-eResearch/nectar_uoa_gpu.

