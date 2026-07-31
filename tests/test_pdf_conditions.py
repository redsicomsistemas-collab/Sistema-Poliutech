import unittest

from utils.pdf_conditions import format_pdf_condition_lines


class PdfConditionFormattingTests(unittest.TestCase):
    def test_clausulas_heading_has_no_bullet(self):
        markup = format_pdf_condition_lines(
            ["CLAUSULAS:", "Condiciones climáticas", "Suspensiones"]
        )

        self.assertEqual(
            markup,
            "CLAUSULAS:<br/>• Condiciones climáticas<br/>• Suspensiones",
        )

    def test_accented_heading_has_no_bullet(self):
        self.assertEqual(format_pdf_condition_lines(["CLÁUSULAS:"]), "CLÁUSULAS:")

    def test_heading_starts_after_a_blank_line(self):
        markup = format_pdf_condition_lines(
            ["Esperando contar con su preferencia.", "CLÁUSULAS:", "Suspensiones"]
        )

        self.assertEqual(
            markup,
            "• Esperando contar con su preferencia.<br/><br/>CLÁUSULAS:<br/>• Suspensiones",
        )

    def test_regular_lines_keep_their_bullets(self):
        self.assertEqual(
            format_pdf_condition_lines(["Cambios de alcance"]),
            "• Cambios de alcance",
        )


if __name__ == "__main__":
    unittest.main()
