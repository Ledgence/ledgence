"""Highlighting must preserve evidence exactly and keep markup inert (MIT)."""
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from change_review.syntax import highlight_snippet


def source_of(rows):
    return ''.join(token['text'] for row in rows for token in row['tokens'])


class SyntaxTests(unittest.TestCase):
    def test_python_preserves_whitespace_unicode_and_untrusted_text(self):
        cases = [
            '', 'return 100', 'await ctx.fork("checks:0", branches=branches)\n',
            '\t# café\r\n\treturn "</script><img src=x onerror=alert(1)>"\r\n',
            'text = """one\ntwo\nthree"""\nreturn text\n',
            'text = "one\vtwo"\nreturn text',
        ]
        for source in cases:
            with self.subTest(source=source):
                rows = highlight_snippet(source)
                self.assertEqual(source_of(rows), source)
                self.assertTrue(all(set(token) == {'kind', 'text'} for row in rows for token in row['tokens']))

    def test_invalid_candidate_stays_readable(self):
        for source in ['return (', 'text = """unfinished', '  pass\n pass\n']:
            with self.subTest(source=source):
                rows = highlight_snippet(source)
                self.assertEqual(source_of(rows), source)
                self.assertTrue(all(token['kind'] == 'plain' for row in rows for token in row['tokens']))

    def test_diff_keeps_headers_markers_and_exact_source(self):
        source = '--- a/pagination.py\n+++ b/pagination.py\n@@ -1 +1 @@\n-return count // 100 + 1\n+return (count + 99) // 100\n\\ No newline at end of file'
        rows = highlight_snippet(source, diff=True)
        self.assertEqual(source_of(rows), source)
        self.assertEqual([row['kind'] for row in rows], ['meta', 'meta', 'meta', 'remove', 'add', 'meta'])
        self.assertEqual(rows[3]['tokens'][0]['text'], '-')
        self.assertEqual(rows[4]['tokens'][0]['text'], '+')


if __name__ == '__main__':
    unittest.main()
