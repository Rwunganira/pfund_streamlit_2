"""
views/admin.py
==============
Admin panel — user management. Only accessible to role='admin'.
"""

import pandas as pd
import streamlit as st

from utils.db import db_list_users, db_set_user_active, db_set_user_role
from utils.helpers import MOBILE_CSS

_ROLES = ["analyst", "manager", "admin"]


def render_admin_panel() -> None:
    st.html(MOBILE_CSS)
    st.title("⚙️ Admin Panel")
    st.caption("User management — visible to admins only.")

    # Guard: double-check role in case someone navigates here directly
    if st.session_state.get("role") != "admin":
        st.error("Access denied.")
        st.stop()

    users = db_list_users()
    if not users:
        st.info("No users found.")
        return

    df = pd.DataFrame(users)
    current_user = st.session_state.get("username", "")

    # ── Summary metrics ───────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Users",       len(df))
    c2.metric("Active",            int(df["is_active"].sum()))
    c3.metric("Email Verified",    int(df["email_verified"].sum()))
    c4.metric("Admins",            int((df["role"] == "admin").sum()))

    st.divider()

    # ── User table ────────────────────────────────────────────────────────────
    st.markdown("#### All Users")
    display_cols = ["username", "name", "email", "role",
                    "is_active", "email_verified", "created_at", "last_login"]
    display_cols = [c for c in display_cols if c in df.columns]
    st.dataframe(df[display_cols], use_container_width=True, height=300)

    st.divider()

    # ── Edit individual user ──────────────────────────────────────────────────
    st.markdown("#### Edit User")
    other_users = [u for u in df["username"].tolist() if u != current_user]
    if not other_users:
        st.info("No other users to manage.")
        return

    sel_user = st.selectbox("Select user", other_users, key="admin_sel_user")
    row = df[df["username"] == sel_user].iloc[0]

    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown(f"**{row['name']}** — `{row['email']}`")
        st.caption(
            f"Role: {row['role']} | "
            f"Active: {'✅' if row['is_active'] else '❌'} | "
            f"Verified: {'✅' if row['email_verified'] else '❌'}"
        )

        # Toggle active
        new_active = st.toggle(
            "Account active",
            value=bool(row["is_active"]),
            key=f"active_{sel_user}",
        )
        if new_active != bool(row["is_active"]):
            db_set_user_active(sel_user, new_active)
            status = "activated" if new_active else "deactivated"
            st.success(f"Account {status}.")
            st.rerun()

    with col_b:
        # Change role
        current_role_idx = _ROLES.index(row["role"]) if row["role"] in _ROLES else 0
        new_role = st.selectbox(
            "Role",
            _ROLES,
            index=current_role_idx,
            key=f"role_{sel_user}",
        )
        if st.button("Apply role change", key=f"apply_role_{sel_user}"):
            if new_role != row["role"]:
                db_set_user_role(sel_user, new_role)
                st.success(f"Role updated to **{new_role}**.")
                st.rerun()
            else:
                st.info("Role unchanged.")
