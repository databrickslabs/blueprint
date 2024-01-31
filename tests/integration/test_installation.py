import pytest


@pytest.mark.parametrize('ext,magic', [
    ('py', "# Databricks notebook source"),
    ('scala', "// Databricks notebook source"),
    ('sql', "-- Databricks notebook source"),
])
def test_uploading_notebooks_get_correct_urls(ext, magic, new_installation):
    remote_path = new_installation.upload(f"foo.{ext}", f"{magic}\nprint(1)".encode("utf8"))
    assert f"{new_installation.install_folder()}/foo" == remote_path
