import os
import sys
import newrelic
import logging

from poaupdater import uLogging, uPEM, uUtil, openapi, uTasks
from apm_settings import *

uLogging.debug = uLogging.log_func(None, uLogging.DEBUG)

logfile = '/usr/local/nr-oa-monitor/oa_monitor.log'
allres = {}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=logfile,
    filemode='a')

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# Chesk tasks

allres["Component/Tasks/All[Tasks]"] 		= uTasks.getNumberOfAllTasks()
allres["Component/Tasks/Active[Tasks]"] 	= uTasks.getNumberOfActiveTasks()
allres["Component/Tasks/Failed[tasks]"]		= uTasks.getNumberOfFailedTasks()
allres["Component/Tasks/Cancelled[tasks]"]	= uTasks.getNumberOfCanceledTasks()
allres["Component/Tasks/Executing[tasks]"]	= uTasks.getNumberOfExecutingTasks()
allres["Component/Tasks/Rescheduled[tasks]"]	= uTasks.getNumberOfRescheduledTasks()
allres["Component/Tasks/Unprocessed[tasks]"]	= uTasks.getNumberOfUnprocessedTasks()
allres["Component/Tasks/Failed[Install]"] 	= uTasks.getNumberOfFailedInstallationTasks()
allres["Component/Tasks/Unprocessed[Install]"] 	= uTasks.getNumberOfUnprocessedInstallationTasks()
allres["Component/TasksLog/All[Tasks]"] 	= uTasks.getNumberOfTasksLogAll()
allres["Component/TasksLog/Success[tasks]"] 	= uTasks.getNumberOfTasksLogSuccess()
allres["Component/TasksLog/Failed[tasks]"] 	= uTasks.getNumberOfTasksLogFailed()
allres["Component/TasksLog/Deleted[tasks]"] 	= uTasks.getNumberOfTasksLogDeleted()
allres["Component/TasksLog/Rescheduled[tasks]"] = uTasks.getNumberOfTasksLogRescheduled()

# Check API

try:
	api = openapi.OpenAPI().pem.getHost(host_id = 1)
	allres["Component/OpenAPI/Availability"] = 1
except:
	allres["Component/OpenAPI/Availability"] = 0

# Check Hosts

hosts = uPEM.getAllHosts()
results = []

for i in hosts:
	res = uPEM.checkOneHostAvailability(i, True)
	results.append((res, i))

not_reachable_hosts = filter(lambda x: x[0], results)

allres["Component/Hosts/All[Hosts]"]		= len(hosts)
allres["Component/Hosts/UI[Hosts]"] 		= len(uPEM.getUIHosts())
allres["Component/Hosts/NotReachable[Hosts]"]	= len(not_reachable_hosts)
allres["Component/Hosts/Win[hosts]"]		= uTasks.getHostsWin()
allres["Component/Hosts/Clusters[clusters]"]	= uTasks.getHostsCluster()
allres["Component/Hosts/VZnode[hosts]"]		= uTasks.getHostsVZnode()
allres["Component/Hosts/NGnode[hosts]"]		= uTasks.getHostsNGnode()
allres["Component/Hosts/NGhost[hosts]"]		= uTasks.getHostsNGhost()
allres["Component/Hosts/Servers[hosts]"]	= uTasks.getHostsServer()
allres["Component/Hosts/VPS[hosts]"]		= uTasks.getHostsVPS()

newrelic_utils = newrelic.Newrelic_Metrics(
            apm_settings.get('nr_proxy'), 
            apm_settings.get('nr_license_key'), 
            apm_settings.get('nr_agent_name'), 
            apm_settings.get('nr_agent_version'), 
            apm_settings.get('nr_guid'), 
            apm_settings.get('nr_poll_cycle'), 
            allres, # here is a metrics we are reporting
            apm_settings.get('nr_hostname')
        )
newrelic_utils.newrelic_post_request()

