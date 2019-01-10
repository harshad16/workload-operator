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

import click
from kubernetes import client
from kubernetes import config
from openshift.dynamic import DynamicClient

from thoth.common import init_logging
from thoth.common import OpenShift
from thoth.common import __version__ as THOTH_COMMON_VERSION

init_logging()

_LOGGER = logging.getLogger("thoth.workload_operator")
_OPENSHIFT = OpenShift()


def _get_method_and_parameters(event) -> typing.Tuple[str, dict]:
    """Get method and parameters from event."""
    _LOGGER.debug(
        "Obtaining method and parameters that should be used for handling workload"
    )

    configmap_name = event["object"].metadata.name
    method = event["object"].data.method

    if not method:
        _LOGGER.error(
            "No method to be called provided workload ConfigMap %r: %r",
            configmap_name,
            event["object"].data,
        )
        raise ValueError

    parameters = event["object"].data.parameters
    if not parameters:
        _LOGGER.error(
            "No parameters supplied in workload ConfigMap %r: %r",
            configmap_name,
            event["object"].data,
        )
        raise ValueError

    try:
        parameters = json.loads(event["object"].data.parameters)
    except Exception as exc:
        _LOGGER.exception(
            "Failed to parse parameters for method call %r in workload ConfigMap %r: %s",
            method,
            configmap_name,
            event["object"].data,
            str(exc),
        )
        raise ValueError

    return method, parameters


def _wait_for_quota():
    """Wait for quota to have resources available."""
    # TODO: implement
    pass


@click.command()
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    envvar="THOTH_OPERATOR_VERBOSE",
    help="Be verbose about what is going on.",
)
@click.option(
    "--operator-namespace",
    "-n",
    type=str,
    required=True,
    envvar="THOTH_OPERATOR_NAMESPACE",
    help="Namespace to connect to to wait for events.",
)
def cli(operator_namespace: str, verbose: bool = False):
    """Operator handling Thoth's workloads."""
    if verbose:
        _LOGGER.setLevel(logging.DEBUG)

    _LOGGER.info("Workload operator is watching namespace %r", operator_namespace)

    config.load_incluster_config()
    dyn_client = DynamicClient(client.ApiClient(configuration=client.Configuration()))
    v1_configmap = dyn_client.resources.get(api_version="v1", kind="ConfigMap")

    for event in v1_configmap.watch(
        namespace=operator_namespace, label_selector="operator=workload"
    ):
        if event["type"] != "ADDED":
            _LOGGER.debug("Skipping event, not addition event (type: %r)", event["type"])
            continue

        configmap_name = event["object"].metadata.name
        _LOGGER.info("Handling event for %r", configmap_name)
        try:
            method_name, method_parameters = _get_method_and_parameters(event)
        except ValueError:
            # Reported in the function call, just continue here.
            continue

        _wait_for_quota()

        # Perform actual method call.
        try:
            method = getattr(_OPENSHIFT, method_name)
            method_result = method(**method_parameters)
        except Exception as exc:
            _LOGGER.exception(
                "Failed to call method %r on Thoth's OpenShift instance in version %r with parameters %r: %s",
                method_name,
                THOTH_COMMON_VERSION,
                method_parameters,
                str(exc),
            )
            continue

        _LOGGER.info(
            "Result of the method call for %r with parameters %r is %r",
            method_name,
            method_parameters,
            method_result,
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


if __name__ == "__main__":
    sys.exit(cli())
