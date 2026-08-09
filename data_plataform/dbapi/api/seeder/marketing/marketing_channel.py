import asyncio
import sys
import os
import uuid
from datetime import datetime
from sqlalchemy import insert
from api.database import AsyncSessionLocal
from api.schemas.marketing.marketing_channel import MarketingChannel, ChannelName, PlatformName
from api.models.marketing.marketing_channel import MarketingChannelModel
from api.seeder.cli_common import build_parser

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

def get_marketing_channel_seeders() -> list[dict]:
    """
    Generates the unique combinations (Wide Dimension) of Channel and Platform.
    """
    batch = []
    combinations = [
        (ChannelName.PAID_SEARCH, PlatformName.GOOGLE_ADS),
        (ChannelName.SOCIAL_MEDIA, PlatformName.META_ADS),
        (ChannelName.SOCIAL_MEDIA, PlatformName.TIKTOK),
        (ChannelName.SOCIAL_MEDIA, PlatformName.LINKEDIN_ADS),
        (ChannelName.EMAIL, PlatformName.MAILCHIMP),
        (ChannelName.ORGANIC, PlatformName.GA4),
        (ChannelName.ORGANIC, PlatformName.NONE),
        (ChannelName.DIRECT, PlatformName.GA4),
        (ChannelName.DIRECT, PlatformName.NONE),
        (ChannelName.AFFILIATE, PlatformName.NONE),
        (ChannelName.OTHER, PlatformName.NONE),
    ]
    today = datetime.now()
    for index, (channel, platform) in enumerate(combinations, start=1):
        payload = {
            "channel_id": index,  # placeholder for Pydantic validation (gt=0); removed before insert
            "channel_name": channel.value,
            "platform_name": platform.value,
            "created_at": today,
            "updated_at": today,
            "is_active": True,
            "row_guid": uuid.uuid4()
        }
        validated_channel = MarketingChannel(**payload).model_dump()
        validated_channel.pop("channel_id", None)  # let the database's autoincrement take over
        batch.append(validated_channel)
    return batch

async def seed_marketing_channels():
    """
    Runs the marketing channels load into the database.
    """
    async with AsyncSessionLocal() as session:
        channels = get_marketing_channel_seeders()
        print(f"📣 Seeding {len(channels)} marketing channels...")
        try:
            stmt = insert(MarketingChannelModel).values(channels)
            await session.execute(stmt)
            await session.commit()
            print("✅ Channels seeded successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding channels: {e}")

if __name__ == "__main__":
    parser = build_parser("Seeder for marketing.marketing_channel (fixed master data, no chaos)")
    parser.parse_args()

    asyncio.run(seed_marketing_channels())
