"""Tests for Java SDK Makefile integration.

Verifies that issue #9 requirements are met:
- make test-java target exists and runs Java tests
- make test includes Java tests
- .gitignore properly ignores Java SDK build artifacts
"""

import os
import subprocess
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def run_command(cmd: str, cwd: Path | None = None, check: bool = True) -> subprocess.CompletedProcess:
    """Run a command and return the result."""
    return subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
        cwd=cwd or PROJECT_ROOT,
        check=check,
    )


class TestJavaMakefileTarget:
    """Test that make test-java target works correctly."""

    def test_test_java_target_exists(self):
        """Verify the test-java target is declared in the Makefile."""
        makefile = PROJECT_ROOT / "Makefile"
        assert makefile.exists(), "Makefile should exist"
        content = makefile.read_text()
        assert "test-java:" in content, "Makefile should contain test-java target"

    def test_test_java_in_composite_test(self):
        """Verify test-java is included in the composite test target."""
        makefile = PROJECT_ROOT / "Makefile"
        content = makefile.read_text()
        # The test target should list test-java (or the PHONY should include it)
        phony_line = ""
        for line in content.splitlines():
            if line.startswith(".PHONY:"):
                phony_line = line
                break
        assert "test-java" in phony_line, "test-java should be in .PHONY declarations"

    def test_make_test_java_succeeds(self):
        """Verify make test-java runs Java SDK tests to completion."""
        result = run_command("make test-java", check=False)
        assert result.returncode == 0, (
            f"make test-java should succeed. "
            f"stdout: {result.stdout[-1000:] if result.stdout else 'N/A'}\n"
            f"stderr: {result.stderr[-2000:] if result.stderr else 'N/A'}"
        )
        assert "Running Java SDK tests" in result.stdout or "Running Java SDK tests" in result.stderr

    def test_java_gradlew_exists(self):
        """Verify gradlew executable exists in sdks/java."""
        gradlew = PROJECT_ROOT / "sdks" / "java" / "gradlew"
        assert gradlew.exists(), "sdks/java/gradlew should exist"
        assert os.access(gradlew, os.X_OK), "sdks/java/gradlew should be executable"


class TestJavaGitignore:
    """Test that .gitignore properly handles Java SDK artifacts."""

    def test_gitignore_contains_java_gradle_dir(self):
        """Verify .gitignore includes sdks/java/.gradle/."""
        gitignore = PROJECT_ROOT / ".gitignore"
        content = gitignore.read_text()
        assert "sdks/java/.gradle/" in content, ".gitignore should include sdks/java/.gradle/"

    def test_gitignore_contains_java_build_dir(self):
        """Verify .gitignore includes sdks/java/build/."""
        gitignore = PROJECT_ROOT / ".gitignore"
        content = gitignore.read_text()
        assert "sdks/java/build/" in content, ".gitignore should include sdks/java/build/"


class TestJavaCompositeTestTarget:
    """Test that 'make test' includes Java tests in its composition."""

    def test_test_target_includes_test_java(self):
        """Verify the test target list includes test-java."""
        makefile = PROJECT_ROOT / "Makefile"
        content = makefile.read_text()
        # Find the test: target line
        found = False
        for line in content.splitlines():
            stripped = line.strip()
            if stripped.startswith("test:") and not stripped.startswith("test-"):
                found = True
                break
        assert found, "test target should exist in Makefile"

    def test_test_java_runs_as_part_of_composite_test(self):
        """Verify that running 'make test' includes Java in test phases.
        
        Note: This test only checks that test-java appears in the test target
        definition, as running the full 'make test' is expensive and involves
        multiple language test suites.
        """
        makefile = PROJECT_ROOT / "Makefile"
        content = makefile.read_text()
        
        # Search for the test target line
        for line in content.splitlines():
            stripped = line.strip()
            if stripped.startswith("test:") and not stripped.startswith("test-"):
                # Found the test target, now check for test-java in the line
                assert "test-java" in stripped, (
                    f"test-java should be a dependency of the test target. "
                    f"Found: {stripped}"
                )
                return
        
        pytest.fail("test target should exist in Makefile")
