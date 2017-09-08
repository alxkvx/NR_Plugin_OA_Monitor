# Steps to configure:
# 1) Place this scripts to Odin Automation MN
# 2) yum install python-requests
# 3) mv apm_settings_example.py apm_settings.py
# 4) specify New Relic license in apm_settings.py and review other settings
# 5) run: python precheckNR.py 
# 6) configure cron job to run it every 2 minutes

# Settings:
#
# If MN is allowed to send through a proxy only
# specify proxy servers as follows:
#
# nr_proxies=dict(
#         http='socks5://user:pass@host:port',
#         https='socks5://user:pass@host:port'
#         )

apm_settings=dict(
        nr_proxies=False,
        nr_license_key='767797c3025c1d7eb6ffee66ff394216bf3feb85',
        nr_agent_name="OAE_demoset",
        nr_agent_version="1.0.0",
        nr_guid="OA_Monitor",
        nr_poll_cycle=300,
        nr_hostname='cloud.serviceproessentials.com',
	nr_insert_key='Fx1M-aGQKg2Pg2gA9Zj7HYZ9XFIW4CrU',
	nr_account_id='1459604'
)

