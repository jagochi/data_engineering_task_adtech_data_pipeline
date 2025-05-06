import psycopg
import os
import random
import datetime
from datetime import date, timedelta, datetime

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "postgres")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")

def get_connection():
    return psycopg.connect(
        f"host={POSTGRES_HOST} port={POSTGRES_PORT} dbname={POSTGRES_DB} user={POSTGRES_USER}",
        autocommit=False,
    )


def create_advertisers(conn, num_advertisers=2):
    """Create a specified number of advertisers."""
    adv_ids = []

    with conn.cursor() as cur:
        for i in range(1, num_advertisers + 1):
            adv_name = f"Advertiser {chr(64 + i)}"  # A, B, C, etc.
            cur.execute(
                """
                INSERT INTO advertiser (name, updated_at)
                VALUES (%s, NOW()) RETURNING id
                """,
                (adv_name,),
            )
            adv_id = cur.fetchone()[0]
            adv_ids.append(adv_id)

    return adv_ids


def create_campaigns(conn, advertiser_ids, campaigns_per_advertiser=3):
    """Create campaigns for each advertiser."""
    campaign_ids = []
    start_date = date.today()

    with conn.cursor() as cur:
        for adv_id in advertiser_ids:
            for i in range(1, campaigns_per_advertiser + 1):
                campaign_name = f"Campaign_{adv_id}_{i}"
                bid = round(random.uniform(0.5, 5.0), 2)
                budget = round(random.uniform(50, 500), 2)
                end_date = start_date + timedelta(days=random.randint(7, 30))

                cur.execute(
                    """
                    INSERT INTO campaign
                        (name, bid, budget, start_date, end_date, advertiser_id, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW()) RETURNING id
                    """,
                    (campaign_name, bid, budget, start_date, end_date, adv_id),
                )
                campaign_id = cur.fetchone()[0]
                campaign_ids.append(campaign_id)

    return campaign_ids


def create_impressions(conn, campaign_ids, impressions_per_campaign=100):
    """Create impressions for campaigns."""
    with conn.cursor() as cur:
        for campaign_id in campaign_ids:
            # Create impressions distributed over the past week
            for _ in range(impressions_per_campaign):
                timestamp = datetime.now() - timedelta(
                    days=random.randint(0, 7),
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                )
                cur.execute(
                    """
                    INSERT INTO impressions (campaign_id, created_at)
                    VALUES (%s, %s)
                    """,
                    (campaign_id, timestamp),
                )


def create_clicks(conn, campaign_ids, click_ratio=0.1):
    """Create clicks for campaigns based on impressions."""
    with conn.cursor() as cur:
        for campaign_id in campaign_ids:
            # Get impressions for this campaign
            cur.execute(
                "SELECT id, created_at FROM impressions WHERE campaign_id = %s", (campaign_id,)
            )
            impressions = cur.fetchall()

            # Create clicks for a percentage of impressions
            for imp_id, imp_time in random.sample(impressions, int(len(impressions) * click_ratio)):
                # Add a small delay after impression (1-120 seconds)
                click_time = imp_time + timedelta(seconds=random.randint(1, 120))
                cur.execute(
                    """
                    INSERT INTO clicks (campaign_id, created_at)
                    VALUES (%s, %s)
                    """,
                    (campaign_id, click_time),
                )


def main(
    num_advertisers=2, campaigns_per_advertiser=3, impressions_per_campaign=100, click_ratio=0.1
):
    """Seed the database with test data."""
    conn = get_connection()
    if not conn:
        print("Could not connect to Postgres. Exiting.")
        return

    with conn:
        # Create advertisers
        print(f"Creating {num_advertisers} advertisers...")
        adv_ids = create_advertisers(conn, num_advertisers)

        # Create campaigns
        print(f"Creating {campaigns_per_advertiser} campaigns per advertiser...")
        campaign_ids = create_campaigns(conn, adv_ids, campaigns_per_advertiser)

        # Create impressions
        print(f"Creating ~{impressions_per_campaign} impressions per campaign...")
        create_impressions(conn, campaign_ids, impressions_per_campaign)

        # Create clicks (based on impressions)
        print(f"Creating clicks with approximately {click_ratio*100:.1f}% CTR...")
        create_clicks(conn, campaign_ids, click_ratio)

        conn.commit()

    conn.close()
    print("Seeding complete!")
    print(f"Created {num_advertisers} advertisers with {len(campaign_ids)} campaigns total.")
    print(
        f"Generated approximately {len(campaign_ids) * impressions_per_campaign} impressions and {len(campaign_ids) * impressions_per_campaign * click_ratio:.0f} clicks."
    )


if __name__ == "__main__":
    main()
