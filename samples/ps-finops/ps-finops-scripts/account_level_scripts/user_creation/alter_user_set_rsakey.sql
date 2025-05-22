ALTER USER <% ctx.env.finops_admin_user %>
   SET RSA_PUBLIC_KEY='<% ctx.env.rsa_public_key %>';
