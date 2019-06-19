#!/usr/bin/env python3
# thoth-workload-operator
# Copyright(C) 2019 Fridolin Pokorny
#
# This program is free software: you can redistribute it and / or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""Operator handling Thoth's workload."""

import sys
import logging
import json
import typing
import time
from multiprocessing import Queue
from multiprocessing import Process

import click
from openshift.dynamic.exceptions import ConflictError

from thoth.common import init_logging
from thoth.common import OpenShift


init_logging()

_LOGGER = logging.getLogger("thoth.workload_operator")


def event_producer(queue: Queue, operator_namespace: str):
    """Queue events to be processed coming from the cluster."""
    _LOGGER.info("Starting event producer")
    openshift = OpenShift()
    v1_configmap = openshift.ocp_client.resources.get(api_version="v1", kind="ConfigMap")
    for event in v1_configmap.watch(namespace=operator_namespace, label_selector="operator=workload"):
        if event["type"] != "ADDED":
            _LOGGER.debug("Skipping event, not addition event (type: %r)", event["type"])
            continue

        configmap_name = event["object"].metadata.name
        _LOGGER.info("Queuing event %r for processing", configmap_name)
        queue.put(configmap_name)


def _get_method_and_parameters(configmap) -> typing.Tuple[str, dict, str, dict]:
    """Get method and parameters from event."""
    _LOGGER.debug("Obtaining method and parameters that should be used for handling workload")

    configmap_name = configmap.metadata.name
    method_name = configmap.data.run_method_name

    if not method_name:
        _LOGGER.error(
            "No method to be called provided workload ConfigMap %r: %r",
            configmap_name,
            configmap.data
        )
        raise ValueError

    parameters = configmap.data.run_method_parameters
    if not parameters:
        _LOGGER.error(
            "No parameters supplied in workload ConfigMap %r: %r",
            configmap_name,
            configmap.data
        )
        raise ValueError

    try:
        parameters = json.loads(parameters)
    except Exception as exc:
        _LOGGER.exception(
            "Failed to parse parameters for method call %r in workload ConfigMap %r: %s",
            method_name,
            configmap_name,
            configmap.data,
            str(exc),
        )
        raise ValueError

    template_method_name = configmap.data.template_method_name
    if not template_method_name:
        _LOGGER.error(
            "No template method name supplied in workload ConfigMap %r: %r", configmap_name, configmap.data
        )
        raise ValueError

    template_method_parameters = configmap.data.template_method_parameters
    if not template_method_parameters:
        _LOGGER.error(
            "No template method parameters supplied in workload ConfigMap %r: %r", configmap_name, configmap.data
        )
        raise ValueError

    try:
        template_method_parameters = json.loads(template_method_parameters)
    except Exception as exc:
        _LOGGER.exception(
            "Failed to parse template parameters for method call %r in workload ConfigMap %r: %s",
            template_method_name,
            configmap_name,
            configmap.data,
            str(exc),
        )
        raise ValueError

    return method_name, parameters, template_method_name, template_method_parameters


@click.command()
@click.option(
    "--verbose", "-v", is_flag=True, envvar="THOTH_OPERATOR_VERBOSE", help="Be verbose about what is going on."
)
@click.option(
    "--sleep-time",
    "-s",
    type=float,
    envvar="THOTH_OPERATOR_SLEEP_TIME",
    help="Sleep for the given time if resources are not available.",
    default=5.0,
)
@click.option(
    "--operator-namespace",
    "-n",
    type=str,
    required=True,
    envvar="THOTH_OPERATOR_NAMESPACE",
    help="Namespace to connect to to wait for events.",
)
def cli(operator_namespace: str, sleep_time: float, verbose: bool = False):
    """Operator handling Thoth's workloads."""
    if verbose:
        _LOGGER.setLevel(logging.DEBUG)

    _LOGGER.info(
        "Workload operator is watching namespace %r with sleep time set to %f seconds", operator_namespace, sleep_time
    )

    queue = Queue()
    producer = Process(target=event_producer, args=(queue, operator_namespace))

    openshift = OpenShift()
    v1_configmap = openshift.ocp_client.resources.get(api_version="v1", kind="ConfigMap")

    producer.start()
    while producer.is_alive():
        configmap_name = queue.get()
        _LOGGER.info("Handling %r", configmap_name)

        # We ask for the event again.
        try:
            configmap = v1_configmap.get(name=configmap_name, namespace=operator_namespace)
        except Exception as exc:
            _LOGGER.exception("Failed to obtain configmap for further processing: %s", str(exc))
            continue

        try:
            method_name, method_parameters, \
                template_method_name, template_method_parameters = _get_method_and_parameters(configmap)
        except ValueError:
            # Reported in the function call, just continue here.
            continue

        # Perform actual method call.
        try:
            template = getattr(openshift, template_method_name)(**template_method_parameters)

            _LOGGER.info("Waiting for resources to become available")
            while not openshift.can_run_workload(template, operator_namespace):
                time.sleep(sleep_time)

            method = getattr(openshift, method_name)
            method_result = method(**method_parameters, template=template)
        except ConflictError as exc:
            _LOGGER.warning("Workload for event %r was already handled: %s", configmap, str(exc))
            continue
        except Exception as exc:
            _LOGGER.exception("Failed run requested workload for event %r: %s", configmap, str(exc))
            continue

        _LOGGER.info("Successfully scheduled workload %r with name %r", method_result, template["metadata"]["name"])
        _LOGGER.debug(
            "Result of the method call for %r with parameters %r is %r", method_name, method_parameters, method_result
        )
        _LOGGER.debug(
            "Deleting ConfigMap %r from namespace %r, workload successfully scheduled",
            configmap_name,
            operator_namespace,
        )
        try:
            v1_configmap.delete(name=configmap_name, namespace=operator_namespace)
        except Exception as exc:
            _LOGGER.exception(
                "Failed to delete workload ConfigMap %r from namespace %r: %s",
                configmap_name,
                operator_namespace,
                str(exc),
            )
            continue

    producer.join()

    # Always fail, this should be run forever.
    sys.exit(1)


if __name__ == "__main__":
    sys.exit(cli())
