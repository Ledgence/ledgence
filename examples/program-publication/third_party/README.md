# Application dependency review

MarkupSafe 3.0.4 is an optional dependency of this example, not the Ledgence
platform, client, or worker helper. Its two reviewed wheels target ordinary
CPython 3.14 on Linux glibc (amd64 and arm64). The native extension is built from
the project's C source. The distributions declare no dependencies and contain
no separate bundled native libraries.

[The published release](https://pypi.org/project/MarkupSafe/3.0.4/) and
[source](https://github.com/pallets/markupsafe/tree/3.0.4) use BSD-3-Clause. It
allows proprietary application code, requires legal notices, and prohibits
implying endorsement. It does not require advertising or product branding.
The hashes and provenance are in `inventory.json`; the unmodified license is in
`MarkupSafe-LICENSE.txt`. pip preserves the wheel's own `.dist-info/licenses`
files in the prepared program. Do not strip those notices when redistributing.
The source and wheels remain under their own license, not Ledgence's MIT license.
