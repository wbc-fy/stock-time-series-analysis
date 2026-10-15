/** Authentication never uses mock data, URL passwords or browser token storage. */
export function createAuthClient({ fetchImpl = globalThis.fetch, base = '' } = {}) {
  let csrf = null
  async function call(path, method = 'GET', body) {
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), 10000)
    try {
      const headers = { Accept: 'application/json' }
      if (method !== 'GET') {
        if (!csrf) throw new Error('会话尚未就绪，请刷新后重试')
        headers['Content-Type'] = 'application/json'
        headers['X-CSRF-Token'] = csrf
      }
      const response = await fetchImpl(base + path, { method, headers, credentials: 'same-origin',
        signal: controller.signal, ...(body === undefined ? {} : { body: JSON.stringify(body) }) })
      if (!response.ok) {
        if (response.status === 401 && path !== '/api/auth/login' && typeof window !== 'undefined') window.dispatchEvent(new Event('sas-auth-required'))
        const messages = { 400: '输入无效，请检查账号、密码和角色', 401: '账号或密码错误，或会话已失效',
          403: '操作无权限，或会话已变更，请刷新后重试', 429: '尝试过于频繁，请在 15 分钟后重试', 503: '认证服务未就绪，请联系管理员检查配置和数据库迁移' }
        const error = new Error(messages[response.status] || `接口返回 HTTP ${response.status}`)
        error.status = response.status
        throw error
      }
      const result = await response.json()
      if (Object.hasOwn(result, 'csrf')) csrf = result.csrf
      return result
    } catch (error) {
      if (error.name === 'AbortError') throw new Error('认证请求超时，请检查服务后重试')
      if (error instanceof TypeError) throw new Error('无法连接认证服务，请检查网络后重试')
      throw error
    } finally { clearTimeout(timer) }
  }
  return {
    session: () => call('/api/auth/session'),
    login: (username, password) => call('/api/auth/login', 'POST', { username, password }),
    logout: () => call('/api/auth/logout', 'POST'),
    password: (old_password, new_password) => call('/api/auth/password', 'POST', { old_password, new_password }),
    users: () => call('/api/admin/users'),
    createUser: body => call('/api/admin/users', 'POST', body),
    updateUser: (id, body) => call(`/api/admin/users/${encodeURIComponent(id)}`, 'PATCH', body),
    clear: () => { csrf = null },
  }
}

export const authApi = createAuthClient({ base: import.meta.env.VITE_API_BASE || '' })
