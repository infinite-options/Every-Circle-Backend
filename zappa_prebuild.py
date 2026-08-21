"""
Zappa packaging helper for cryptography on AWS Lambda.

`cryptography` ships native extensions. A Mac venv installs macOS wheels, which
Lambda (Amazon Linux x86_64) cannot load. Zappa runs `install_lambda_crypto`
before zipping so the package contains manylinux wheels.

After `zappa update`, this venv may have Linux binaries and local
`python3 ec_api.py` will fail until you restore macOS wheels:

    pip install --force-reinstall 'cryptography==41.0.7' 'cffi==1.17.1'
"""

from __future__ import annotations

import os
import shutil
import site
import subprocess
import sys
import sysconfig
import tempfile

CRYPTO_VERSION = "41.0.7"
CFFI_VERSION = "1.17.1"
LAMBDA_PLATFORM = "manylinux2014_x86_64"
LAMBDA_PYTHON = "3.10"

CRYPTO_RESTORE_HINT = """
cryptography native extensions failed to load (wrong OS wheel).

If this is your Mac after `zappa update`, restore local wheels:
  pip install --force-reinstall 'cryptography==41.0.7' 'cffi==1.17.1'

If this is AWS Lambda, redeploy so zappa_prebuild.install_lambda_crypto
packages manylinux x86_64 wheels (python3.10).
""".strip()


def is_lambda_wheel_crypto_error(exc: BaseException) -> bool:
    """True when cryptography failed because the wheel is for another OS/arch."""
    msg = f"{type(exc).__name__}: {exc}".lower()
    clues = (
        "mach-o",
        "incompatible architecture",
        "invalid elf header",
        "wrong elf class",
        "exec format error",
        "_rust.abi3",
        "cryptography.hazmat.bindings._rust",
        "dlopen",
        "image not found",
        "no suitable image found",
    )
    return any(token in msg for token in clues)


def exit_if_crypto_broken() -> None:
    """Import cryptography and abort with a restore hint if the wheel is wrong."""
    try:
        from cryptography.hazmat.backends import default_backend  # noqa: F401
        from cryptography.hazmat.primitives.ciphers import Cipher  # noqa: F401
        from cryptography.hazmat.primitives.padding import PKCS7  # noqa: F401
    except Exception as exc:
        if is_lambda_wheel_crypto_error(exc):
            print(CRYPTO_RESTORE_HINT, file=sys.stderr)
            sys.exit(1)
        raise


def _site_packages() -> str:
    paths = []
    try:
        paths.extend(site.getsitepackages())
    except Exception:
        pass
    purelib = sysconfig.get_path("purelib")
    if purelib:
        paths.append(purelib)
    for path in paths:
        if path and os.path.isdir(path) and "site-packages" in path:
            return path
    raise RuntimeError("Could not locate the venv site-packages directory")


def _pkg_version(name: str, default: str) -> str:
    try:
        from importlib.metadata import version

        return version(name)
    except Exception:
        return default


def _copy_tree(src: str, dest: str) -> None:
    if os.path.isdir(dest):
        shutil.rmtree(dest)
    elif os.path.exists(dest):
        os.remove(dest)
    if os.path.isdir(src):
        shutil.copytree(src, dest)
    else:
        shutil.copy2(src, dest)


def install_lambda_crypto() -> None:
    """Zappa `prebuild_script`: swap macOS cryptography wheels for manylinux."""
    crypto_version = _pkg_version("cryptography", CRYPTO_VERSION)
    cffi_version = _pkg_version("cffi", CFFI_VERSION)
    dest = _site_packages()
    print(
        f"Installing Lambda manylinux wheels into {dest}: "
        f"cryptography=={crypto_version}, cffi=={cffi_version}"
    )

    tmp = tempfile.mkdtemp(prefix="zappa-lambda-crypto-")
    try:
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--no-cache-dir",
                "--upgrade",
                "--only-binary=:all:",
                "--platform",
                LAMBDA_PLATFORM,
                "--python-version",
                LAMBDA_PYTHON,
                "--implementation",
                "cp",
                "--target",
                tmp,
                f"cryptography=={crypto_version}",
                f"cffi=={cffi_version}",
            ]
        )
        for name in os.listdir(tmp):
            _copy_tree(os.path.join(tmp, name), os.path.join(dest, name))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print(
        "Installed manylinux cryptography for Lambda. "
        "Restore Mac wheels after deploy with:\n"
        f"  pip install --force-reinstall 'cryptography=={crypto_version}' "
        f"'cffi=={cffi_version}'"
    )
