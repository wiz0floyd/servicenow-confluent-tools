"""End-to-end `sn-confluent setup` wizard.

Orchestrates extract -> link -> mirror -> replicate in a single run, holding
state in-memory only (no resume after failure). Each step is a thin shim that
synthesises an argv slice and calls the relevant subcommand's `main(argv)`.

Inputs are collected upfront so the wizard knows what to skip and where to
write artifacts. The user can deselect individual steps before execution
(e.g. when PEMs already exist and `extract` is not needed).
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from typing import Callable, List, Optional

STEP_NAMES = ("extract", "link", "mirror", "replicate")


@dataclass
class WizardState:
    pem_dir: str = "."
    link_conf: str = ""
    keystore: str = "keystore"
    truststore: str = "truststore"
    selected_steps: List[str] = field(default_factory=lambda: list(STEP_NAMES))


def _import_step(name: str) -> Callable[[Optional[List[str]]], int]:
    if name == "extract":
        from sn_confluent.extract.main import main
    elif name == "link":
        from sn_confluent.link.main import main
    elif name == "mirror":
        from sn_confluent.mirror.main import main
    elif name == "replicate":
        from sn_confluent.replicate.main import main
    else:
        raise KeyError(name)
    return main


def _build_argv(step: str, state: WizardState) -> List[str]:
    if step == "extract":
        return [
            "--keystore", state.keystore,
            "--truststore", state.truststore,
            "--out-dir", state.pem_dir,
        ]
    common = ["--config", state.link_conf, "--pem-dir", state.pem_dir]
    return common  # link/mirror/replicate all take the same shared flags


def _run_step(step: str, state: WizardState) -> int:
    print()
    print("=" * 60)
    print(f"  Step: sn-confluent {step}")
    print("=" * 60)
    main_fn = _import_step(step)
    argv = _build_argv(step, state)
    try:
        rc = main_fn(argv)
    except SystemExit as exc:
        rc = int(exc.code) if exc.code is not None else 0
    return rc if isinstance(rc, int) else 0


def _collect_inputs(state: WizardState) -> bool:
    """Prompt for shared inputs. Returns True on success, False if user cancels."""
    import questionary

    selected = questionary.checkbox(
        "Select steps to run (space to toggle, enter to confirm):",
        choices=[questionary.Choice(title=name, value=name, checked=True) for name in STEP_NAMES],
    ).ask()
    if not selected:
        print("No steps selected.")
        return False
    state.selected_steps = selected

    state.pem_dir = (
        questionary.text("PEM output / input directory:", default=state.pem_dir).ask() or state.pem_dir
    )

    if any(s in selected for s in ("link", "mirror", "replicate")):
        default_conf = state.link_conf or os.path.join("sn_confluent", "link", "link.conf")
        state.link_conf = questionary.text(
            "Path to link.conf:", default=default_conf
        ).ask() or default_conf

    if "extract" in selected:
        state.keystore = questionary.text("Keystore path:", default=state.keystore).ask() or state.keystore
        state.truststore = questionary.text("Truststore path:", default=state.truststore).ask() or state.truststore

    return True


def main(argv: Optional[List[str]] = None) -> int:
    print("sn-confluent setup wizard")
    print("Orchestrates: extract -> link -> mirror -> replicate")
    print()

    state = WizardState()
    if not _collect_inputs(state):
        return 0

    print()
    print(f"Plan: run {', '.join(state.selected_steps)}")
    print(f"  pem_dir   = {state.pem_dir}")
    if any(s in state.selected_steps for s in ("link", "mirror", "replicate")):
        print(f"  link.conf = {state.link_conf}")
    if "extract" in state.selected_steps:
        print(f"  keystore  = {state.keystore}")
        print(f"  truststore= {state.truststore}")

    import questionary
    if not questionary.confirm("Proceed?", default=True).ask():
        print("Aborted.")
        return 0

    for step in state.selected_steps:
        rc = _run_step(step, state)
        if rc != 0:
            print(f"\nStep '{step}' failed with exit code {rc}.")
            cont = questionary.confirm("Continue with remaining steps?", default=False).ask()
            if not cont:
                return rc

    print()
    print("=" * 60)
    print("  setup complete")
    print("=" * 60)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
