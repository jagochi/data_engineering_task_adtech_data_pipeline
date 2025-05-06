#!/usr/bin/env python
"""Command line scripts for the data-task project."""

import os
import sys
import time
import subprocess
import argparse


def run_command(cmd):
    """Run a shell command and print output."""
    print(f"Running: {cmd}")
    process = subprocess.run(cmd, shell=True)
    return process.returncode


def up():
    """Start Docker containers."""
    print("Starting Docker containers...")
    run_command("docker-compose up -d")
    print("Waiting for services to be ready...")
    time.sleep(5)  # Give some time for services to initialize


def down():
    """Stop Docker containers."""
    print("Stopping Docker containers...")
    run_command("docker-compose down")


def reset():
    """Remove all Docker containers and volumes."""
    print("Removing all containers and volumes...")
    run_command("docker-compose down -v")


def clean():
    """Remove all Docker containers, volumes, and build cache."""
    print("Performing a complete cleanup...")
    run_command("docker-compose down -v")
    run_command("docker system prune -f")


def ps():
    """Check the status of Docker containers."""
    run_command("docker-compose ps")


def logs():
    """View logs for all services or a specific service."""
    parser = argparse.ArgumentParser(description="View Docker logs")
    parser.add_argument("--service", help="Specific service to see logs for")
    args = parser.parse_args()

    if args.service:
        run_command(f"docker-compose logs {args.service}")
    else:
        run_command("docker-compose logs")


def seed():
    """Seed the database with test data."""
    parser = argparse.ArgumentParser(description="Seed database with test data")
    parser.add_argument("--advertisers", type=int, default=2, help="Number of advertisers")
    parser.add_argument("--campaigns", type=int, default=3, help="Campaigns per advertiser")
    parser.add_argument("--impressions", type=int, default=100, help="Impressions per campaign")
    parser.add_argument("--ctr", type=float, default=0.1, help="Click-through rate (0.0-1.0)")
    args = parser.parse_args()

    print(
        f"Seeding database with {args.advertisers} advertisers, {args.campaigns} campaigns each..."
    )
    cmd = (
        f"python main.py batch --advertisers {args.advertisers} --campaigns {args.campaigns} "
        f"--impressions {args.impressions} --ctr {args.ctr}"
    )
    run_command(cmd)


def stats():
    """Show database statistics."""
    run_command("python main.py stats")


def reset_data():
    """Reset database data without removing containers."""
    run_command("python main.py reset")
    print("All data has been reset.")


def setup():
    """Start containers and seed database."""
    up()

    # Wait for PostgreSQL to be ready
    print("Waiting for PostgreSQL to be ready...")
    max_retries = 10
    retry_interval = 2
    for i in range(max_retries):
        try:
            subprocess.run(
                ["docker", "exec", "psql_source", "pg_isready", "-U", "postgres"],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            print("PostgreSQL is ready!")
            break
        except subprocess.CalledProcessError:
            print(
                f"PostgreSQL not ready yet, retrying in {retry_interval} seconds... ({i+1}/{max_retries})"
            )
            time.sleep(retry_interval)
    else:
        print("Failed to connect to PostgreSQL after several attempts.")
        return

    # Wait a bit more for Flyway migrations to complete
    print("Waiting for database migrations to complete...")
    time.sleep(5)

    # Seed the database using default values
    run_command("python main.py batch")

    print("\nEnvironment is ready! Running stats:")
    stats()
    print("\nAccess PostgreSQL: localhost:5432 (user: postgres, password: postgres)")
    print("Access ClickHouse: localhost:8123 (HTTP) or localhost:9000 (TCP)")
