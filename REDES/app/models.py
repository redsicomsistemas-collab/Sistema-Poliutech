from dataclasses import dataclass


@dataclass
class GeneratedContent:
    image_summary: str
    facebook_copy: str
    linkedin_copy: str
    hashtags: str
    cta: str
    alt_text: str
