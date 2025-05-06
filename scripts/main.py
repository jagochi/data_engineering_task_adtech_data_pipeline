#!/usr/bin/env python

import argparse
import sys
from seed import (
    get_connection,
    create_advertisers,
    create_campaigns,
    create_impressions,
    create_clicks,
)

def parse_args():
    parser = argparse.ArgumentParser(description="AdTech Data Generator")

    # Main command subparsers
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Create advertisers command
    adv_parser = subparsers.add_parser("advertisers", help="Create advertisers")
    adv_parser.add_argument("--count", type=int, default=1, help="Number of advertisers to create")

    # Create campaigns command
    camp_parser = subparsers.add_parser("campaigns", help="Create campaigns")
    camp_parser.add_argument(
        "--advertiser-id", type=int, required=True, help="Advertiser ID to create campaigns for"
    )
    camp_parser.add_argument("--count", type=int, default=1, help="Number of campaigns to create")

    # Create impressions command
    imp_parser = subparsers.add_parser("impressions", help="Create impressions")
    imp_parser.add_argument(
        "--campaign-id", type=int, required=True, help="Campaign ID to create impressions for"
    )
    imp_parser.add_argument(
        "--count", type=int, default=100, help="Number of impressions to create"
    )

    # Create clicks command
    click_parser = subparsers.add_parser("clicks", help="Create clicks")
    click_parser.add_argument(
        "--campaign-id", type=int, required=True, help="Campaign ID to create clicks for"
    )
    click_parser.add_argument("--ratio", type=float, default=0.1, help="Click ratio (0.0-1.0)")

    # Batch generation command
    batch_parser = subparsers.add_parser("batch", help="Generate a batch of test data")
    batch_parser.add_argument("--advertisers", type=int, default=2, help="Number of advertisers")
    batch_parser.add_argument("--campaigns", type=int, default=3, help="Campaigns per advertiser")
    batch_parser.add_argument(
        "--impressions", type=int, default=100, help="Impressions per campaign"
    )
    batch_parser.add_argument("--ctr", type=float, default=0.1, help="Click-through rate (0.0-1.0)")

    # Show stats command
    subparsers.add_parser("stats", help="Show database statistics")

    # Reset command
    subparsers.add_parser("reset", help="Reset all data (USE WITH CAUTION)")

    return parser.parse_args()


def show_stats(conn):
    """Display current database statistics."""
    with conn.cursor() as cur:
        print("=== Database Statistics ===")

        # Count advertisers
        cur.execute("SELECT COUNT(*) FROM advertiser")
        adv_count = cur.fetchone()[0]
        print(f"Advertisers: {adv_count}")

        # Count campaigns
        cur.execute("SELECT COUNT(*) FROM campaign")
        camp_count = cur.fetchone()[0]
        print(f"Campaigns: {camp_count}")

        # Count impressions
        cur.execute("SELECT COUNT(*) FROM impressions")
        imp_count = cur.fetchone()[0]
        print(f"Impressions: {imp_count}")

        # Count clicks
        cur.execute("SELECT COUNT(*) FROM clicks")
        click_count = cur.fetchone()[0]
        print(f"Clicks: {click_count}")

        # Overall CTR
        if imp_count > 0:
            ctr = (click_count / imp_count) * 100
            print(f"Overall CTR: {ctr:.2f}%")

        # Campaign details
        print("\n=== Campaign Details ===")
        cur.execute(
            """
            SELECT 
                c.id, 
                c.name, 
                a.name as advertiser, 
                COUNT(DISTINCT i.id) as impressions,
                COUNT(DISTINCT cl.id) as clicks
            FROM campaign c
            JOIN advertiser a ON c.advertiser_id = a.id
            LEFT JOIN impressions i ON c.id = i.campaign_id
            LEFT JOIN clicks cl ON c.id = cl.campaign_id
            GROUP BY c.id, c.name, a.name
            ORDER BY c.id
        """
        )

        print(
            f"{'ID':<5} {'Name':<20} {'Advertiser':<15} {'Impressions':<12} {'Clicks':<8} {'CTR':<6}"
        )
        print("-" * 70)

        for row in cur.fetchall():
            camp_id, camp_name, adv_name, imps, clicks = row
            ctr = (clicks / imps * 100) if imps > 0 else 0
            print(
                f"{camp_id:<5} {camp_name[:20]:<20} {adv_name[:15]:<15} {imps:<12} {clicks:<8} {ctr:.2f}%"
            )


def reset_data(conn):
    """Reset all data in the database."""
    confirmation = input("This will DELETE ALL DATA. Type 'yes' to confirm: ")
    if confirmation.lower() != "yes":
        print("Operation cancelled.")
        return

    with conn.cursor() as cur:
        print("Deleting all data...")
        cur.execute("DELETE FROM clicks")
        cur.execute("DELETE FROM impressions")
        cur.execute("DELETE FROM campaign")
        cur.execute("DELETE FROM advertiser")
        conn.commit()
        print("All data has been deleted.")


def main():
    args = parse_args()

    if not args.command:
        print("No command specified. Use --help for options.")
        sys.exit(1)

    conn = get_connection()
    if not conn:
        print("Could not connect to Postgres. Exiting.")
        sys.exit(1)

    try:
        if args.command == "advertisers":
            adv_ids = create_advertisers(conn, args.count)
            conn.commit()
            print(f"Created {len(adv_ids)} advertisers. IDs: {adv_ids}")

        elif args.command == "campaigns":
            campaign_ids = create_campaigns(conn, [args.advertiser_id], args.count)
            conn.commit()
            print(
                f"Created {len(campaign_ids)} campaigns for advertiser #{args.advertiser_id}. IDs: {campaign_ids}"
            )

        elif args.command == "impressions":
            create_impressions(conn, [args.campaign_id], args.count)
            conn.commit()
            print(f"Created {args.count} impressions for campaign #{args.campaign_id}")

        elif args.command == "clicks":
            # First get impressions count
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM impressions WHERE campaign_id = %s", (args.campaign_id,)
                )
                imp_count = cur.fetchone()[0]
                if imp_count == 0:
                    print(
                        f"No impressions found for campaign #{args.campaign_id}. Creating clicks requires impressions."
                    )
                    return

            create_clicks(conn, [args.campaign_id], args.ratio)
            conn.commit()
            print(f"Created clicks for campaign #{args.campaign_id} with {args.ratio*100:.1f}% CTR")

        elif args.command == "batch":
            from seed import main as seed_main

            seed_main(args.advertisers, args.campaigns, args.impressions, args.ctr)

        elif args.command == "stats":
            show_stats(conn)

        elif args.command == "reset":
            reset_data(conn)

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    finally:
        conn.close()


if __name__ == "__main__":
    main()
