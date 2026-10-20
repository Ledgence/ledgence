# Ledgence identity

Curvature is Ledgence’s symbol: a single asymmetric saddle surface, with the
upper-right tip raised. These files are the public vector masters. The plain
symbol and the shaded illustration share the same outline.

## Choose an asset

| Asset | Use |
| --- | --- |
| `mark.svg` | Interface symbol; uses `currentColor`, including as a CSS mask. |
| `mark-dark.svg`, `mark-light.svg` | Standalone symbol for light or dark backgrounds. |
| `mark-silver.svg` | Large illustrations; avoid shading at favicon sizes. |
| `logo-dark.svg`, `logo-light.svg` | Horizontal symbol and outlined lowercase wordmark. |
| `favicon.svg` | Browser icon; adapts to the browser’s preferred color scheme. |
| `avatar-dark-500.png`, `avatar-light-500.png` | GitHub, Gravatar and other account profiles. |
| `avatar-*-1024.png` | Larger profile exports. |
| `apple-touch-icon.png` | Home screen icon. |
| `social-github.png` | 1280 × 640 repository social preview. |
| `ledgence-logo-v1.png` | 1200 × 320 banner for registry and repository READMEs. |

Avatar SVGs include additional space for circular crops. Do not remove that
padding. The 500px exports leave more than 30px between the symbol and the
inscribed circle. `small-size-proof.png` shows the flat symbol at 16, 24, 32,
48 and 64 pixels on both backgrounds.

## Consistent use

Preserve the shape and aspect ratio. Do not rotate, mirror, stretch or add a
border to the symbol. Give it clear space of at least one sixth of its visible
height. Use the plain mark in interfaces, leaving the sculptural treatment for
larger artwork. Pair a dark mark with a light background and vice versa.

The palette is graphite `#202126`, near-black `#0C0D10`, off-white `#F5F5F7`,
silver `#747C89`, and light silver `#B9C0CE`. The wordmark is Inter Regular,
converted to paths so it renders without loading a font. Inter is by the Inter
Project Authors under the SIL Open Font License; see `INTER-LICENSE.txt`.

## Keeping surfaces aligned

`geometry.json` records the canonical outline and colors. Update the vector
masters here first. Keep copies in `docs-site/public/brand`,
`console/public/brand`, and the landing repository’s `public/brand` synchronized.
The portable examples embed the same outline in
`examples/codex-change-review/change_review/report.py` and
`examples/fulfillment-investigator/fulfillment/report.py`. Regenerate the landing’s
saved player without changing its execution evidence when the shell changes.

Registry README images use the versioned public URL
`https://ledgence.com/brand/ledgence-logo-v1.png`. Publish that asset before
publishing a package containing the reference. Existing package versions keep
their original README; account avatars are managed independently. GitHub’s
organization and package-owner user have separate avatars. PyPI and Docker Hub
use their configured Gravatar identity; crates.io uses its owner’s GitHub avatar.

The brand artwork is supplied under the accompanying MIT license. Do not use it
to imply endorsement or affiliation. This asset kit makes no trademark
registration or legal-clearance claim.
