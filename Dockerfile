# Step 1: Build the React app
FROM node as build
WORKDIR /app
COPY Frontend/package.json ./
RUN npm install
COPY Frontend/ .
RUN npm run build

# Step 2: Serve with Nginx
FROM nginx:alpine
COPY --from=build /app/build /usr/share/nginx/html
# Copy custom Nginx configuration
COPY Frontend/nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 3000
CMD ["nginx", "-g", "daemon off;"]
