"""
Utility methods related to a remote or local host.

Most of the methods take a fabric connection as a parameter.
Read: http://docs.fabfile.org/en/2.4/api/connection.html
"""
import logging
import socket

logger = logging.getLogger(__name__)


def service_stop(c, service):
    """Stop a service."""
    logger.info("Stopping %s on %s", service, hostname(c.host))
    cmd = (
        "service {} stop".format(service) if _systemd(c) else "stop {}".format(service)
    )
    run(c, "sudo {}".format(cmd))


def service_start(c, service):
    """Start a service."""
    logger.info("Starting %s on %s", service, hostname(c.host))
    cmd = (
        "service {} start".format(service)
        if _systemd(c)
        else "start {}".format(service)
    )
    run(c, "sudo {}".format(cmd))


def run(c, cmd):
    """Run a command on the host."""
    logger.info("Running command %s on %s", cmd, hostname(c.host))
    result = c.run(cmd, hide="out")
    logger.info(
        "\033[1;37mhost: %s, cmd: %s, exit_status: %d\033[0m",
        c.host,
        result.command,
        result.return_code,
    )
    logger.debug("cmd_result: %s", result)


def hostname(ip):
    """
    Return hostname for given ip or ip itself if reverse lookup fails.
    """
    try:
        data = socket.gethostbyaddr(ip)
        return repr(data[0])
    except socket.herror:
        logger.debug("Failed to lookup hostname for ip %s", ip)
        return ip


def _systemd(c):
    """Return true if connected host is systemd based."""
    # process with pid 1 is the service manager, its name
    # is being used as an indicator.
    result = c.run("ps --no-headers -o comm 1", hide="both")
    return result.stdout.strip() == "systemd"  # 'init' when upstart
