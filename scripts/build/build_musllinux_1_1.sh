#!/bin/bash

# Create musllinux_1_1 wheels for psycopg2
#
# Look at the .github/workflows/packages.yml file for hints about how to use it.

set -euo pipefail
set -x

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
prjdir="$( cd "${dir}/../.." && pwd )"

# Build all the available versions, or just the ones specified in PYVERS
if [ ! "${PYVERS:-}" ]; then
    PYVERS="$(ls /opt/python/)"
fi

# Don't build musl wheels for Python 3.9 and 3.10 which is the version on
# Alpine 3.14 and 3.15. The will install but cause 'ImportError' because of
# conflicting SOAPI values (wanting '-musl.so' but getting '-gnu.so').
#
# https://discuss.python.org/t/a-mess-with-soabi-tags-on-musllinux/11688
PYVERS=${PYVERS//cp39-cp39/}
PYVERS=${PYVERS//cp310-cp310/}

# Find psycopg version
version=$(grep -e ^PSYCOPG_VERSION "${prjdir}/setup.py" | sed "s/.*'\(.*\)'/\1/")
# A gratuitous comment to fix broken vim syntax file: '")
distdir="${prjdir}/dist/psycopg2-$version"

# Replace the package name
if [[ "${PACKAGE_NAME:-}" ]]; then
    sed -i "s/^setup(name=\"psycopg2\"/setup(name=\"${PACKAGE_NAME}\"/" \
        "${prjdir}/setup.py"
fi

# Install prerequisite libraries
apk update
apk add postgresql-dev tzdata

# Create the wheel packages
for pyver in $PYVERS; do
    pybin="/opt/python/${pyver}/bin"
    "${pybin}/python" -m build -w -o "${prjdir}/dist/" "${prjdir}"
done

# Bundle external shared libraries into the wheels
for whl in "${prjdir}"/dist/*.whl; do
    auditwheel repair "$whl" -w "$distdir"
done

# Install packages and test
cd "${prjdir}"
for pyver in $PYVERS; do
    pybin="/opt/python/${pyver}/bin"
    "${pybin}/pip" install ${PACKAGE_NAME:-psycopg2} --no-index -f "$distdir"

    # Print psycopg and libpq versions
    "${pybin}/python" -c "import psycopg2; print(psycopg2.__version__)"
    "${pybin}/python" -c "import psycopg2; print(psycopg2.__libpq_version__)"
    "${pybin}/python" -c "import psycopg2; print(psycopg2.extensions.libpq_version())"

    # Fail if we are not using the expected libpq library
    if [[ "${WANT_LIBPQ:-}" ]]; then
        "${pybin}/python" -c "import psycopg2, sys; sys.exit(${WANT_LIBPQ} != psycopg2.extensions.libpq_version())"
    fi

    "${pybin}/python" -c "import tests; tests.unittest.main(defaultTest='tests.test_suite')"
done
