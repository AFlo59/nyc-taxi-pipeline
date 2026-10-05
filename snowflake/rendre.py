"""Remplace les variables Jinja des scripts SQL par les valeurs du .env.

Usage, depuis la racine du projet :
    python snowflake/rendre.py snowflake/01_infrastructure.sql
    python snowflake/rendre.py --tous

Le SQL final est écrit dans snowflake/build/ (ignoré par Git) : l'ouvrir,
tout copier, puis le coller dans une feuille Snowsight.

Valeurs : fichier .env à la racine du projet ; une variable SNOWFLAKE_*
définie dans le terminal est prioritaire.
Variable calculée : SNOWFLAKE_RSA_PUBLIC_KEY, lue dans le fichier indiqué par
SNOWFLAKE_PUBLIC_KEY_PATH (clé publique, sans les lignes BEGIN et END).
La clé privée n'est jamais lue.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

from dotenv import dotenv_values
from jinja2 import Environment, StrictUndefined, TemplateSyntaxError, meta

RACINE = Path(__file__).resolve().parent.parent
DOSSIER_SQL = RACINE / "snowflake"
DOSSIER_BUILD = DOSSIER_SQL / "build"

# Variables insérées telles quelles comme noms d'objets Snowflake : on
# refuse tout ce qui n'est pas un identifiant simple (pas d'espace, pas de ;)
IDENTIFIANTS = (
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_RESOURCE_MONITOR",
)
IDENTIFIANT_VALIDE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]{0,254}$")
CLE_VALIDE = re.compile(r"^[A-Za-z0-9+/=]+$")

ENVIRONNEMENT = Environment(
    undefined=StrictUndefined,  # une variable absente arrête le rendu
    keep_trailing_newline=True,
    trim_blocks=True,  # pas de lignes vides laissées par les {% if %}
    lstrip_blocks=True,
)


def lire_cle_publique(chemin: Path) -> str:
    """Contenu de la clé publique sur une seule ligne, sans BEGIN ni END."""
    lignes = chemin.read_text(encoding="utf-8").splitlines()
    return "".join(l.strip() for l in lignes if l.strip() and "-----" not in l)


def charger_variables() -> dict[str, str]:
    variables = {
        nom: valeur
        for nom, valeur in dotenv_values(RACINE / ".env").items()
        if valeur is not None
    }
    # Le terminal est prioritaire, uniquement pour les variables du projet
    variables.update({n: v for n, v in os.environ.items() if n.startswith("SNOWFLAKE_")})

    if not variables.get("SNOWFLAKE_RSA_PUBLIC_KEY"):
        chemin = Path(
            variables.get("SNOWFLAKE_PUBLIC_KEY_PATH") or "~/.ssh/snowflake/rsa_key.pub"
        ).expanduser()
        variables["SNOWFLAKE_RSA_PUBLIC_KEY"] = lire_cle_publique(chemin) if chemin.is_file() else ""
    return variables


def erreurs_de_valeurs(variables: dict[str, str]) -> list[str]:
    erreurs = []
    for nom in IDENTIFIANTS:
        valeur = variables.get(nom)
        if valeur and not IDENTIFIANT_VALIDE.match(valeur):
            erreurs.append(f"{nom}={valeur!r} : nom d'objet Snowflake invalide")
    quota = variables.get("SNOWFLAKE_CREDIT_QUOTA")
    if quota and not quota.isdigit():
        erreurs.append(f"SNOWFLAKE_CREDIT_QUOTA={quota!r} : nombre entier attendu")
    cle = variables.get("SNOWFLAKE_RSA_PUBLIC_KEY", "")
    if cle and not CLE_VALIDE.match(cle):
        erreurs.append("SNOWFLAKE_RSA_PUBLIC_KEY : ce n'est pas une clé publique (base64)")
    return erreurs


def rendre(fichier: Path, variables: dict[str, str]) -> Path:
    texte = fichier.read_text(encoding="utf-8")
    try:
        arbre = ENVIRONNEMENT.parse(texte)
    except TemplateSyntaxError as e:
        raise SystemExit(f"{fichier.name}, ligne {e.lineno} : erreur de syntaxe Jinja ({e.message})")

    # Toutes les variables manquantes d'un coup, plutôt qu'une par une
    manquantes = sorted(meta.find_undeclared_variables(arbre) - variables.keys())
    if manquantes:
        raise SystemExit(
            f"{fichier.name} : variable(s) absente(s) du .env : {', '.join(manquantes)}\n"
            "  → les ajouter à partir de .env.example"
        )

    DOSSIER_BUILD.mkdir(exist_ok=True)
    sortie = DOSSIER_BUILD / fichier.name
    sortie.write_text(ENVIRONNEMENT.from_string(texte).render(variables), encoding="utf-8")
    return sortie


def main() -> int:
    parser = argparse.ArgumentParser(description="Rend les gabarits SQL avec les valeurs du .env.")
    parser.add_argument("fichiers", nargs="*", type=Path, help="gabarits SQL à rendre")
    parser.add_argument("--tous", action="store_true", help="rendre tous les snowflake/NN_*.sql")
    args = parser.parse_args()

    fichiers = sorted(DOSSIER_SQL.glob("[0-9][0-9]_*.sql")) if args.tous else args.fichiers
    if not fichiers:
        parser.error("indiquer un ou plusieurs fichiers SQL, ou --tous")
    introuvables = [str(f) for f in fichiers if not f.is_file()]
    if introuvables:
        parser.error(f"fichier(s) introuvable(s) : {', '.join(introuvables)}")

    variables = charger_variables()
    erreurs = erreurs_de_valeurs(variables)
    if erreurs:
        print("Valeurs invalides dans le .env :", *erreurs, sep="\n  - ", file=sys.stderr)
        return 1
    if not variables["SNOWFLAKE_RSA_PUBLIC_KEY"]:
        print("Attention : clé publique introuvable, la ligne ALTER USER ... RSA_PUBLIC_KEY "
              "ne sera pas générée (voir SNOWFLAKE_PUBLIC_KEY_PATH).", file=sys.stderr)

    for fichier in fichiers:
        sortie = rendre(fichier, variables)
        print(f"{fichier.name} → {sortie.relative_to(RACINE)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
