from __future__ import annotations

import unittest

from bankara_cross_encoder_rerank import (
    apply_cross_encoder_fallback,
    parse_cross_encoder_response,
    prepare_cross_encoder_candidate,
    rerank_matches_with_client,
)


class FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text


class FakeModels:
    def __init__(self, text: str) -> None:
        self._text = text

    def generate_content(self, **_: object) -> FakeResponse:
        return FakeResponse(self._text)


class FakeClient:
    def __init__(self, text: str) -> None:
        self.models = FakeModels(text)


class ErrorModels:
    def generate_content(self, **_: object) -> FakeResponse:
        raise RuntimeError("boom")


class ErrorClient:
    def __init__(self) -> None:
        self.models = ErrorModels()


class CrossEncoderRerankTests(unittest.TestCase):
    def test_parse_cross_encoder_response_handles_fenced_json(self) -> None:
        payload = parse_cross_encoder_response(
            '```json\n{"results":[{"index":0,"score":0.91,"reason":"exact lane match"}]}\n```'
        )
        self.assertEqual(len(payload["results"]), 1)
        self.assertEqual(payload["results"][0]["index"], 0)
        self.assertEqual(payload["results"][0]["score"], 0.91)

    def test_prepare_candidate_uses_summary_text(self) -> None:
        candidate = prepare_cross_encoder_candidate(
            {
                "id": "abc",
                "combined_score": 0.82,
                "semantic_score": 0.77,
                "metadata": {
                    "title": "もしも最恐の母が教師になったら",
                    "curation_cohort": "mother-profession",
                    "curation_subcohort": "mother-profession-school-authority",
                    "brain_summary_text_v1": "premise: mother dominates classroom",
                    "notes": "生徒を威圧しながら授業を進める",
                },
            },
            index=0,
        )
        self.assertEqual(candidate["title"], "もしも最恐の母が教師になったら")
        self.assertEqual(candidate["lane"], "mother-profession-school-authority")
        self.assertIn("mother dominates classroom", candidate["searchable_summary"])

    def test_rerank_matches_with_client_scores_and_sorts(self) -> None:
        matches = [
            {
                "id": "wrong",
                "semantic_score": 0.91,
                "combined_score": 0.95,
                "metadata": {
                    "title": "もしも最恐の母が警察官になったら",
                    "curation_subcohort": "mother-profession-law-authority",
                },
            },
            {
                "id": "right",
                "semantic_score": 0.89,
                "combined_score": 0.93,
                "metadata": {
                    "title": "もしも最恐の母が教師になったら",
                    "curation_subcohort": "mother-profession-school-authority",
                },
            },
        ]
        client = FakeClient(
            '{"results":['
            '{"index":0,"score":0.10,"reason":"wrong authority lane"},'
            '{"index":1,"score":0.95,"reason":"exact teacher lane"}'
            ']}'
        )
        reranked = rerank_matches_with_client(client, "教師として校長と生徒を支配する母", matches, top_k=2, score_weight=0.2)
        self.assertEqual(reranked[0]["id"], "right")
        self.assertGreater(reranked[0]["reranked_combined_score"], reranked[1]["reranked_combined_score"])
        self.assertEqual(reranked[0]["cross_encoder_reason"], "exact teacher lane")

    def test_fallback_keeps_scores_when_model_fails(self) -> None:
        matches = [
            {
                "id": "x",
                "semantic_score": 0.75,
                "combined_score": 0.81,
                "metadata": {"title": "sample"},
            }
        ]
        reranked = rerank_matches_with_client(ErrorClient(), "query", matches, top_k=1)
        self.assertEqual(reranked[0]["reranked_combined_score"], 0.81)
        self.assertEqual(reranked[0]["cross_encoder_score"], 0.0)
        self.assertIn("cross-encoder unavailable", reranked[0]["cross_encoder_reason"])

    def test_apply_cross_encoder_fallback_populates_required_fields(self) -> None:
        fallback = apply_cross_encoder_fallback(
            [{"combined_score": 0.88, "metadata": {"title": "sample"}}],
            "temporary error",
        )
        self.assertEqual(fallback[0]["cross_encoder_score"], 0.0)
        self.assertEqual(fallback[0]["cross_encoder_reason"], "temporary error")
        self.assertEqual(fallback[0]["reranked_combined_score"], 0.88)


if __name__ == "__main__":
    unittest.main()
